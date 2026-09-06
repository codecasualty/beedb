package com.memcache;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * Proves the client read path frames commands correctly rather than relying on one
 * read() returning one whole command.
 *
 * WHY THIS EXISTS
 *
 * Server.read() allocates a single 4096-byte ByteBuffer, does ONE socketChannel.read(),
 * and then indexes the value straight out of it. That is not a framing implementation;
 * it is an assumption that TCP delivers messages, which TCP does not do. It appears to
 * work only because loopback has a 65536 MTU, so everything arrives in one segment. A
 * 1500-MTU interface would break it at ~1400 bytes.
 *
 * HOW IT PROVES ANYTHING
 *
 * Every case here writes a command in DELIBERATE FRAGMENTS with a pause between them,
 * forcing the server to see a partial command and wait. A test that writes the whole
 * command in one go passes against the broken code too -- which is exactly how this
 * bug survived until now. If you change nothing else, keep the flush-and-sleep.
 *
 * HOW TO RUN
 *
 *   cd beedb-server && make up
 *   mvn -q dependency:build-classpath -Dmdep.outputFile=target/cp.txt
 *   java -cp "target/classes:target/test-classes:$(cat target/cp.txt)" com.memcache.FramingProbe
 *
 * Needs a live 3-node cluster because SET has to reach a leader.
 */
public class FramingProbe {

    private static final int[] PORTS = { 11211, 11212, 11213 };
    /** Long enough that the server cannot still be waiting on the rest of the segment. */
    private static final int FRAGMENT_PAUSE_MS = 120;

    private static int passed = 0, failed = 0;

    public static void main(String[] args) throws Exception {
        int leader = findLeader();
        if (leader < 0) { System.out.println("no leader; is the cluster up?"); return; }
        System.out.println("leader on port " + leader + "\n");

        wholeCommandAtOnce(leader);
        commandLineSplitAcrossWrites(leader);
        valueSplitAcrossWrites(leader);
        splitBetweenLineAndValue(leader);
        oneByteAtATime(leader);
        twoCommandsInOneWrite(leader);
        oversizedValue(leader);

        System.out.printf("%n%d passed, %d failed%n", passed, failed);
        System.out.println(failed == 0
                ? "PASS -- framing holds under fragmentation"
                : "FAIL -- see above");
    }

    /** Baseline. This one passes even against the broken code; that is the point. */
    private static void wholeCommandAtOnce(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            c.write("set frag_a 0 0 5\r\nhello\r\n");
            check("whole command in one write", "STORED", c.readLine());
        }
    }

    /** The command LINE arrives in pieces. The server must not parse until it sees \r\n. */
    private static void commandLineSplitAcrossWrites(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            c.write("set frag_b 0 ");
            c.pause();
            c.write("0 5\r\nworld\r\n");
            check("command line split mid-header", "STORED", c.readLine());
        }
    }

    /** The VALUE arrives in pieces. This is the case that kills the current code. */
    private static void valueSplitAcrossWrites(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            c.write("set frag_c 0 0 11\r\nhello");
            c.pause();
            c.write(" world\r\n");
            check("value split across writes", "STORED", c.readLine());
        }
    }

    /** Header complete, value not started. The server must hold state between reads. */
    private static void splitBetweenLineAndValue(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            c.write("set frag_d 0 0 6\r\n");
            c.pause();
            c.write("abcdef\r\n");
            check("split exactly between line and value", "STORED", c.readLine());
        }
    }

    /** The pathological case: every byte its own segment. */
    private static void oneByteAtATime(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            for (byte b : "set frag_e 0 0 4\r\nbyte\r\n".getBytes(StandardCharsets.UTF_8)) {
                c.writeRaw(new byte[]{ b });
                Thread.sleep(2);
            }
            check("one byte per write", "STORED", c.readLine());
        }
    }

    /**
     * Two commands in a single write.
     *
     * The trap: after handling the first, the second is ALREADY in the buffer. If the
     * server clears the buffer instead of consuming exactly what it used, or waits for
     * another readable event that never comes (no more bytes are arriving), the second
     * command is silently lost and this hangs.
     */
    private static void twoCommandsInOneWrite(int port) throws Exception {
        try (Conn c = new Conn(port)) {
            c.write("set frag_f 0 0 3\r\none\r\nset frag_g 0 0 3\r\ntwo\r\n");
            check("pipelined #1", "STORED", c.readLine());
            check("pipelined #2", "STORED", c.readLine());
        } catch (Exception e) {
            fail("two commands in one write", "STORED then STORED", e.toString());
        }
    }

    /**
     * A value larger than the server can hold.
     *
     * Two things must be true: a sensible error, and THE NODE MUST STILL BE ALIVE.
     * Before the fix, 100 KB here killed the process outright -- the surviving check
     * is the one that matters.
     */
    private static void oversizedValue(int port) throws Exception {
        String big = "x".repeat(100_000);
        String reply;
        try (Conn c = new Conn(port)) {
            c.write("set frag_big 0 0 " + big.length() + "\r\n" + big + "\r\n");
            reply = c.readLine();
        } catch (Exception e) {
            reply = "<connection died: " + e + ">";
        }
        System.out.println("  oversized value      -> " + reply);

        // The real assertion: can the node still serve a request?
        try (Conn c = new Conn(port)) {
            c.write("get frag_a\r\n");
            String line = c.readLine();
            check("node survives an oversized value", true, line != null && line.startsWith("VALUE"));
        } catch (Exception e) {
            fail("node survives an oversized value", "still serving", "node is gone: " + e);
        }
    }

    // ── plumbing ────────────────────────────────────────────────────────

    private static int findLeader() {
        for (int p : PORTS) {
            try (Conn c = new Conn(p)) {
                c.write("stats\r\n");
                String line;
                while ((line = c.readLine()) != null && !line.equals("END")) {
                    if (line.equals("STAT raft_role LEADER")) return p;
                }
            } catch (Exception ignored) { }
        }
        return -1;
    }

    private static void check(String name, Object expected, Object actual) {
        if (expected.equals(actual)) { passed++; System.out.printf("  %-36s ok%n", name); }
        else fail(name, expected, actual);
    }

    private static void fail(String name, Object expected, Object actual) {
        failed++;
        System.out.printf("  %-36s FAILED  expected %s, got %s%n", name, expected, actual);
    }

    /** A socket with a byte-level readLine, so nothing buffers ahead of us. */
    private static final class Conn implements AutoCloseable {
        private final Socket socket;
        private final InputStream in;
        private final OutputStream out;

        Conn(int port) throws Exception {
            socket = new Socket("127.0.0.1", port);
            socket.setSoTimeout(5000);
            socket.setTcpNoDelay(true);   // do not let Nagle re-merge our fragments
            in = socket.getInputStream();
            out = socket.getOutputStream();
        }

        void write(String s) throws Exception { writeRaw(s.getBytes(StandardCharsets.UTF_8)); }

        void writeRaw(byte[] b) throws Exception { out.write(b); out.flush(); }

        /** Flush, then wait, so the bytes genuinely arrive as separate reads. */
        void pause() throws Exception { out.flush(); Thread.sleep(FRAGMENT_PAUSE_MS); }

        String readLine() throws Exception {
            StringBuilder sb = new StringBuilder();
            int ch;
            while ((ch = in.read()) != -1) {
                if (ch == '\n') break;
                if (ch != '\r') sb.append((char) ch);
            }
            return (ch == -1 && sb.isEmpty()) ? null : sb.toString();
        }

        public void close() { try { socket.close(); } catch (Exception ignored) { } }
    }
}
