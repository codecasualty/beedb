package com.memcache.raft;

import com.memcache.cache.Cache;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.wal.WalRecord;
import com.memcache.raft.wal.WalService;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Drives one follower's handleAppendEntries directly, then restarts it to check WAL replay matches memory. */
public class AppendEntriesVerifyTest {

    @Rule public TemporaryFolder folder = new TemporaryFolder();
    private String stateDir, snapshotDir, tmpDir, walDir;
    private final List<RaftNode> nodes = new ArrayList<>();
    private final List<WalService> extraWals = new ArrayList<>();

    @Before public void setUp() throws IOException {
        stateDir = folder.newFolder("state").getAbsolutePath();
        snapshotDir = folder.newFolder("snap").getAbsolutePath();
        tmpDir = folder.newFolder("tmp").getAbsolutePath();
        walDir = folder.newFolder("wal").getAbsolutePath();
    }

    @After public void tearDown() {
        for (RaftNode n : nodes) try { n.stop(); } catch (Exception ignored) {}
        for (WalService w : extraWals) try { w.shutdown(); } catch (Exception ignored) {}
    }


    private RaftNode node(RaftTransport t, int snap, int eMin, int eMax) {
        RaftNode n = new RaftNode(List.of("peer"), "f", new Cache(), t, stateDir, snapshotDir, tmpDir, walDir,
                snap, snap, eMin, eMax, 50, 200, 1000);
        nodes.add(n);
        return n;
    }
    /** Follower that never starts an election on its own during a test. */
    private RaftNode quietNode(int snap) { return node(new InMemoryRaftTransport(), snap, 60_000, 120_000); }

    private static LogEntry e(int idx, long term) { return new LogEntry(idx, "cmd", term, false, "r" + idx); }
    private static List<LogEntry> range(int from, int to, long term) {
        List<LogEntry> l = new ArrayList<>();
        for (int i = from; i <= to; i++) l.add(e(i, term));
        return l;
    }
    private static AppendEntriesRequest ae(long term, int prev, long prevTerm, int commit, List<LogEntry> entries) {
        return new AppendEntriesRequest(term, "leader", prev, prevTerm, commit, new ArrayList<>(entries));
    }
    private int walRecords() throws IOException {
        Path p = Paths.get(walDir, "f", "wal.log");
        return (int) Files.readAllLines(p).stream().filter(s -> !s.isBlank()).count();
    }
    /** Every slot holds the entry for that index, and nothing extra. */
    private static void assertPositional(String what, RaftLog log) {
        int first = log.getLastIncludedIndex();
        assertEquals(what + ": entries in list vs lastIndex (duplicates?) " + log,
                log.lastIndex() - first, log.size() - 1);
        for (int i = first + 1; i <= log.lastIndex(); i++)
            assertEquals(what + ": slot " + i + " holds wrong index " + log, i, log.get(i).getIndex());
    }
    private static String terms(RaftLog log) {
        StringBuilder sb = new StringBuilder();
        for (int i = log.getLastIncludedIndex() + 1; i < log.getLastIncludedIndex() + log.size(); i++)
            sb.append(log.get(i).getIndex()).append(":t").append(log.get(i).getTerm()).append(' ');
        return sb.toString().trim();
    }
    /** Stop the node and build a fresh one on the same dirs: what a restart replays. */
    private RaftLog restartAndReplay(RaftNode old) {
        old.stop();
        return quietNode(1000).getLog();
    }

    /** Wraps the node's real WAL; the first `slowCalls` appends complete `delayMs` late. */
    static class SlowWal extends WalService {
        final WalService real; final int slowCalls; final long delayMs; final AtomicInteger calls = new AtomicInteger();
        final AtomicInteger slowDone = new AtomicInteger();
        SlowWal(String dummyPath, String tmp, WalService real, int slowCalls, long delayMs) throws IOException {
            super(dummyPath, tmp); this.real = real; this.slowCalls = slowCalls; this.delayMs = delayMs;
        }
        @Override public CompletableFuture<Void> append(WalRecord r) {
            CompletableFuture<Void> f = real.append(r);
            if (calls.getAndIncrement() >= slowCalls) return f;
            return f.thenCompose(v -> CompletableFuture.runAsync(() -> slowDone.incrementAndGet(),
                    CompletableFuture.delayedExecutor(delayMs, TimeUnit.MILLISECONDS)));
        }
        @Override public void shutdown() { super.shutdown(); real.shutdown(); }
    }
    private SlowWal slowWal(RaftNode n, int slowCalls, long delayMs) throws IOException {
        SlowWal w = new SlowWal(folder.newFolder().getAbsolutePath() + "/dummy.log", tmpDir, n.walService, slowCalls, delayMs);
        extraWals.add(w); extraWals.add(n.walService);
        n.walService = w;
        return w;
    }

    //resends same batch does not duplicates the entires in the wal or the memory

    @Test 
    public void resendOfSameBatchWritesNothing() throws Exception {
        RaftNode f = quietNode(1000);
        AppendEntriesResponse r1 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));
        int after1 = walRecords();
        AppendEntriesResponse r2 = null;
        for (int i = 0; i < 100; i++) r2 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));
        assertTrue(r1.isSuccess()); assertTrue(r2.isSuccess());
        assertEquals(5, r1.getMatchIndex()); assertEquals(5, r2.getMatchIndex());
        assertEquals(5, after1);
        assertEquals("100 resends must not grow the WAL", 5, walRecords());
        assertPositional("memory", f.getLog());
        assertPositional("replay", restartAndReplay(f));
    }

    @Test 
    public void overlappingBatchAppendsOnlyTheNewTail() throws Exception {
        RaftNode f = quietNode(1000);
        f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));
        AppendEntriesResponse r = f.handleAppendEntries(ae(1, 2, 1, 0, range(3, 8, 1)));
        assertTrue(r.isSuccess()); assertEquals(8, r.getMatchIndex());
        assertEquals("5 + only 6..8", 8, walRecords());
        assertEquals(8, f.getLog().lastIndex());
        assertPositional("memory", f.getLog());
        assertPositional("replay", restartAndReplay(f));
    }

    @Test 
    public void midBatchConflictTruncatesAtTheConflict() throws Exception {
        RaftNode f = quietNode(1000);
        f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));
        List<LogEntry> batch = new ArrayList<>(List.of(e(3, 1)));
        batch.addAll(range(4, 6, 2));
        AppendEntriesResponse r = f.handleAppendEntries(ae(2, 2, 1, 0, batch));
        assertTrue(r.isSuccess()); assertEquals(6, r.getMatchIndex());
        assertEquals("5 + TRUNCATE + 4..6", 9, walRecords());
        RaftLog log = f.getLog();
        assertEquals("1:t1 2:t1 3:t1 4:t2 5:t2 6:t2", terms(log));
        assertPositional("memory", log);
        RaftLog replayed = restartAndReplay(f);
        assertEquals("replay", "1:t1 2:t1 3:t1 4:t2 5:t2 6:t2", terms(replayed));
    }

    @Test 
    public void heartbeatOverStaleTailReportsOnlyVerifiedPrefix() throws Exception {
        RaftNode f = quietNode(1000);
        f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));
        AppendEntriesResponse r = f.handleAppendEntries(ae(2, 3, 1, 0, List.of()));
        assertTrue(r.isSuccess());
        assertEquals("matchIndex must be prevLogIndex, not the stale tail", 3, r.getMatchIndex());
        assertEquals("heartbeat writes nothing", 5, walRecords());
    }

    // commitIndex <= index of last new entri

    @Test 
    public void commitIndexNeverCoversAStaleTail() throws Exception {
        RaftNode f = quietNode(1000);
        f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 5, 1)));   // 4,5 will turn out stale
        f.handleAppendEntries(ae(2, 3, 1, 5, List.of()));         // leader has committed ITS 4,5
        int commit = Integer.parseInt(f.getStats().get("raft_commit_index"));
        assertTrue("commitIndex " + commit + " covers stale entries 4,5 (Raft: min(leaderCommit, prev+entries))",
                commit <= 3);
    }

    //  WAL wait times out, leader retries 3

    @Test 
    public void walTimeoutThenRetryStillReplaysCorrectly() throws Exception {
        RaftNode f = quietNode(1000);
        slowWal(f, 3, 6000);                                     // batch of 3 takes 6s > the 5s wait
        AppendEntriesResponse r1 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 3, 1)));
        assertFalse("first attempt should time out", r1.isSuccess());
        AppendEntriesResponse r2 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 3, 1)));
        assertTrue(r2.isSuccess());
        assertPositional("memory", f.getLog());
        int recs = walRecords();
        RaftLog replayed = restartAndReplay(f);
        assertPositional("replay after timeout+retry (WAL has " + recs + " records for 3 entries)", replayed);
    }

    // resend of a batch the follower has already snapshotted 

    @Test 
    public void resendBelowFollowersSnapshotIsHarmless() throws Exception {
        RaftNode f = quietNode(5);                               
        List<LogEntry> batch = range(1, 3, 1);
        batch.addAll(range(4, 8, 2));
        AppendEntriesResponse r1 = f.handleAppendEntries(ae(2, 0, 0, 8, batch));
        assertTrue(r1.isSuccess());
        long deadline = System.currentTimeMillis() + 5000;
        while (f.getLog().getLastIncludedIndex() == 0 && System.currentTimeMillis() < deadline) Thread.sleep(20);
        assertTrue("follower should have snapshotted", f.getLog().getLastIncludedIndex() > 0);
        Thread.sleep(200);
        int before = walRecords();
        // the leader never saw r1 (RPC timed out), so it resends the same request
        AppendEntriesResponse r2;
        try {
            r2 = f.handleAppendEntries(ae(2, 0, 0, 8, batch));
        } catch (Exception ex) {
            fail("resend threw " + ex + " after writing " + (walRecords() - before)
                    + " WAL records; lastIncludedIndex=" + f.getLog().getLastIncludedIndex()
                    + " lastIncludedTerm=" + f.getLog().getLastIncludedTerm());
            return;
        }
        assertTrue(r2.isSuccess());
        assertEquals(8, r2.getMatchIndex());
        assertEquals("resend must not write to the WAL", before, walRecords());
    }

    // InstallSnapshot over a stale tail, then new entries 

    @Test 
    public void installSnapshotOverStaleTailThenAppendReplaysCorrectly() throws Exception {
        RaftNode f = quietNode(1000);
        f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 10, 1)));  // uncommitted, will be overwritten
        InstallSnapshotResponse s = f.handleInstallSnapshot(
                new InstallSnapshotRequest(2, "leader", 5, 2, new HashMap<>(), "snap"));
        assertTrue(s.isSuccess());
        AppendEntriesResponse r = f.handleAppendEntries(ae(2, 5, 2, 5, range(6, 8, 2)));
        assertTrue(r.isSuccess());
        assertEquals("memory", "6:t2 7:t2 8:t2", terms(f.getLog()));
        RaftLog replayed = restartAndReplay(f);
        assertEquals("replay after restart", "6:t2 7:t2 8:t2", terms(replayed));
    }

    // the gap between block 1 and block 2 

    /** A peer that grants every vote and accepts every append. */
    static class YesPeer implements RaftTransport {
        public RpcResult<RequestVoteResponse> sendRequestVoteToPeer(RequestVoteRequest q, String p) {
            RequestVoteResponse r = new RequestVoteResponse(); r.setTerm(q.getTerm()); r.setVoteGranted(true); r.setFollowerId(p);
            return RpcResult.ok(r);
        }
        public RpcResult<AppendEntriesResponse> sendAppendEntriesToPeer(AppendEntriesRequest q, String p) {
            AppendEntriesResponse r = new AppendEntriesResponse(); r.setTerm(q.getTerm()); r.setSuccess(true);
            r.setMatchIndex(q.getPrevLogIndex() + q.getEntries().size()); r.setFollowerId(p);
            return RpcResult.ok(r);
        }
        public RpcResult<InstallSnapshotResponse> sendInstallSnapshotToPeer(InstallSnapshotRequest q, String p) {
            return RpcResult.unreachable("n/a");
        }
    }

    @Test 
    public void electionDuringWalWaitDoesNotCorruptTheLog() throws Exception {
        RaftNode f = node(new YesPeer(), 1000, 200, 300);        // election fires 200-300ms after block 1
        slowWal(f, 3, 1500);                                     // the batch's fsync takes 1.5s
        AppendEntriesResponse r = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 3, 1)));
        Thread.sleep(300);
        RaftLog log = f.getLog();
        String state = "role=" + f.getRole() + " term=" + f.getTerm() + " reply=" + r + " log=" + terms(log);
        if (f.getRole() == NodeRole.LEADER) {
            boolean hasOwnEntry = false;
            for (int i = 1; i <= log.lastIndex(); i++) if (log.get(i).getTerm() == f.getTerm()) hasOwnEntry = true;
            assertTrue("leader lost its own no-op to the old leader's batch: " + state, hasOwnEntry);
        }
        assertPositional("memory " + state, log);
        RaftLog replayed = restartAndReplay(f);
        assertEquals("replay must equal memory; " + state, terms(log), terms(replayed));
    }

    // retry must not acknowledge entries that are not on disk yet 

    @Test 
    public void retryAfterWalTimeoutDoesNotAckUnpersistedEntries() throws Exception {
        RaftNode f = quietNode(1000);
        SlowWal w = slowWal(f, 3, 7000);                         
        AppendEntriesResponse r1 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 3, 1)));  
        assertFalse(r1.isSuccess());
        AppendEntriesResponse r2 = f.handleAppendEntries(ae(1, 0, 0, 0, range(1, 3, 1)));   
        int durable = w.slowDone.get();
        if (r2.isSuccess())
            assertEquals("retry answered success matchIndex=" + r2.getMatchIndex()
                    + " while only " + durable + " of 3 entries were fsynced", 3, durable);
        Thread.sleep(2500);
    }
}
