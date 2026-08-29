package com.memcache.raft;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import com.memcache.cache.Cache;

public class SocketClusterHarness implements AutoCloseable {

    public static final class Node {
        public final String    nodeId;
        public final int       raftPort;
        public final List<String> peers;      // "127.0.0.1:<raftPort>" of the OTHER nodes
        public final Path      dir;           // per-node state/wal/snapshot/tmp root

        public RaftNode      raftNode;
        public RaftRpcServer rpcServer;
        public SocketRaftTransport raftTransport;
        public Cache         cache;
        public boolean       running;

        Node(String nodeId, int raftPort, List<String> peers, Path dir) {
            this.nodeId   = nodeId;
            this.raftPort = raftPort;
            this.peers    = peers;
            this.dir      = dir;
        }
    }

    private int snapShotLimit             = 1000;
    private int snapShotThreshold         = 1000;
    private int electionTimeoutMinMs      = 1000;
    private int electionTimeoutMaxMs      = 2000;
    private int heartbeatIntervalMs       = 300;
    private int peerRetryBackoffInitialMs = 200;
    private int peerRetryBackoffMaxMs     = 1000;

    private final List<Node> nodes = new ArrayList<>();
    private final Path root;

    public SocketClusterHarness(int nodeCount, Path root) throws IOException {
        this.root = root;
        int[] ports = freePorts(nodeCount);

        for (int i = 0; i < nodeCount; i++) {
            String nodeId = "node" + (i + 1);
            List<String> peers = new ArrayList<>();
            for (int j = 0; j < nodeCount; j++) {
                if (j != i) peers.add("127.0.0.1:" + ports[j]);
            }
            Path dir = root.resolve(nodeId);
            for (String sub : new String[]{"state", "snapshots", "tmp", "wal"}) {
                Files.createDirectories(dir.resolve(sub));
            }
            nodes.add(new Node(nodeId, ports[i], peers, dir));
        }
    }

    /** Override timings before start() if a test needs to provoke elections faster. */
    public SocketClusterHarness withTimings(int electionMinMs, int electionMaxMs, int heartbeatMs) {
        this.electionTimeoutMinMs = electionMinMs;
        this.electionTimeoutMaxMs = electionMaxMs;
        this.heartbeatIntervalMs  = heartbeatMs;
        return this;
    }

    /**
     * Lower the snapshot thresholds before start() so a test can reach the
     * InstallSnapshot path without writing a thousand entries.
     *
     * Set these small (RaftClusterTest uses 5) and a follower that misses a
     * handful of writes falls below the leader's lastIncludedIndex, which is
     * the condition replicationLoopForPeer checks before choosing a snapshot
     * over entries. Leave them at the default to test ordinary log catch-up
     * instead -- they are different code paths and a test should pick one.
     */
    public SocketClusterHarness withSnapshot(int limit, int threshold) {
        this.snapShotLimit     = limit;
        this.snapShotThreshold = threshold;
        return this;
    }

    /** Every node the harness knows about, running or stopped. */
    public List<Node> nodes()       { return nodes; }

    /**
     * Only the nodes currently up.
     *
     * Use this, not nodes(), any time you are waiting for something to become
     * true -- a stopped node never will, and you will burn the whole timeout
     * on a node you deliberately killed.
     */
    public List<Node> runningNodes() {
        return nodes.stream().filter(n -> n.running).collect(Collectors.toList());
    }

    /** True when every RUNNING node has {@code key} in its cache. */
    public boolean allHaveKey(String key) {
        return runningNodes().stream().allMatch(n -> n.cache.get(key) != null);
    }
    public Node node(String nodeId) {
        return nodes.stream().filter(n -> n.nodeId.equals(nodeId)).findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("no such node: " + nodeId));
    }

    public void start() throws IOException, InterruptedException {
        for (Node n : nodes) startNode(n);
    }

    private void startNode(Node n) throws IOException, InterruptedException {
        n.cache    = new Cache();
        n.raftTransport = new SocketRaftTransport(n.peers);
        n.raftNode = new RaftNode(
                n.peers, n.nodeId, n.cache, n.raftTransport,
                n.dir.resolve("state").toString(),
                n.dir.resolve("snapshots").toString(),
                n.dir.resolve("tmp").toString(),
                n.dir.resolve("wal").toString(),
                snapShotLimit, snapShotThreshold,
                electionTimeoutMinMs, electionTimeoutMaxMs, heartbeatIntervalMs,
                peerRetryBackoffInitialMs, peerRetryBackoffMaxMs);
        // Constructor binds the ServerSocket; start() spawns the accept loop.
        n.rpcServer = new RaftRpcServer(n.raftPort, n.raftNode);
        n.rpcServer.start();
        n.raftNode.start();
        n.running = true;
    }

    /**
     * Stop one node, leaving its state/wal on disk so restart() resumes from it.
     * This is how you provoke the divergent-log path that InMemoryRaftTransport
     * can never reach.
     */
    public void stopNode(String nodeId) {
        Node n = node(nodeId);
        if (!n.running) return;
        n.raftNode.stop();
        closeListener(n);
        n.running = false;
    }

    /** Bring a stopped node back up against the same on-disk state. 
     * @throws InterruptedException */
    public void restartNode(String nodeId) throws IOException, InterruptedException {
        Node n = node(nodeId);
        if (n.running) throw new IllegalStateException(nodeId + " is already running");
        startNode(n);
    }

    // ---------- polling helpers: never Thread.sleep-then-assert ----------

    /**
     * Poll until the condition holds. Fails with the supplied description rather than
     * a bare timeout, because "no leader after 15s" and "commit stalled at 47" need
     * different investigations.
     */
    public void awaitUntil(String description, long timeoutMs, Callable<Boolean> condition) {
        long deadline = System.nanoTime() + timeoutMs * 1_000_000L;
        Throwable last = null;
        while (System.nanoTime() < deadline) {
            try {
                if (Boolean.TRUE.equals(condition.call())) return;
            } catch (Throwable t) {
                last = t;   // a node mid-restart can throw; keep polling
            }
            sleepQuietly(25);
        }
        throw new AssertionError("timed out after " + timeoutMs + "ms waiting for: " + description
                + (last == null ? "" : "  (last error: " + last + ")"), last);
    }

    /** Wait for exactly one leader, and return it. */
    public RaftNode awaitLeader(long timeoutMs) {
        awaitUntil("a single elected leader", timeoutMs, () -> leaders().size() == 1);
        return leaders().get(0);
    }

    public RaftNode awaitFollower(long timeoutMs) {
        awaitUntil("a single elected follower", timeoutMs, () -> followers().size() >= 1);
        return followers().get(0);
    }

    public List<RaftNode> leaders() {
        return nodes.stream()
                .filter(n -> n.running && n.raftNode.getRole() == NodeRole.LEADER)
                .map(n -> n.raftNode)
                .collect(Collectors.toList());
    }

    public List<RaftNode> followers() {
        return nodes.stream()
                .filter(n -> n.running && n.raftNode.getRole() == NodeRole.FOLLOWER)
                .map(n -> n.raftNode)
                .collect(Collectors.toList());
    }

    public Map<String, String> stats(String nodeId) {
        return new LinkedHashMap<>(node(nodeId).raftNode.getStats());
    }

    public long stat(String nodeId, String key) {
        String v = stats(nodeId).get(key);
        if (v == null) throw new IllegalArgumentException("no stat '" + key + "' on " + nodeId);
        return Long.parseLong(v);
    }

    /** True when every running node has applied at least {@code index}. */
    public boolean allApplied(int index) {
        return nodes.stream().filter(n -> n.running)
                .allMatch(n -> Long.parseLong(n.raftNode.getStats().get("raft_last_applied")) >= index);
    }

    // ---------- teardown ----------

    /**
     * Stops every node AND closes every listener.
     */
    @Override
    public void close() {
        for (Node n : nodes) {
            try { if (n.running) n.raftNode.stop(); } catch (Throwable ignored) { }
        }
        for (Node n : nodes) {
            closeListener(n);
            n.running = false;
        }
    }

    private void closeListener(Node n) {
        if (n.rpcServer != null) n.rpcServer.close();
        if (n.raftTransport != null) n.raftTransport.close();
    }

    // ---------- ports ----------

    /**
     * Bind N ephemeral ports, note them, release them, hand them back.
     *
     * There is a race here -- something else could claim a port between close and
     * rebind -- but hard-coding 11211-11213 guarantees a collision on a dev box or in
     * CI, which is worse and harder to diagnose.
     */
    private static int[] freePorts(int count) throws IOException {
        List<ServerSocket> held = new ArrayList<>();
        int[] ports = new int[count];
        try {
            for (int i = 0; i < count; i++) {
                ServerSocket s = new ServerSocket(0);
                s.setReuseAddress(true);
                held.add(s);
                ports[i] = s.getLocalPort();
            }
        } finally {
            for (ServerSocket s : held) {
                try { s.close(); } catch (IOException ignored) { }
            }
        }
        return ports;
    }

    private static void sleepQuietly(long ms) {
        try { Thread.sleep(ms); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
