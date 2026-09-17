package com.memcache.raft;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.cache.CacheItem;
import com.memcache.command.Command;
import com.memcache.command.CommandType;

/**
 * Fault injection: the ONE property a consensus store is not allowed to break.
 *
 *   If the client was told STORED, the value is still there afterwards --
 *   no matter what died in between.
 *
 */
public class DurabilityIT {

    @Rule public TemporaryFolder folder = new TemporaryFolder();

    private static final Logger LOGGER = LoggerFactory.getLogger(DurabilityIT.class.getName());

    private static final int WRITERS          = 8;
    private static final int WRITE_TIMEOUT_MS = 5_000;
    private static final int CONVERGE_MS      = 30_000;

    private SocketClusterHarness cluster;

    @Before
    public void setUp() throws Exception {
        cluster = new SocketClusterHarness(3, folder.getRoot().toPath());
    }

    @After
    public void tearDown() {
        if (cluster != null) cluster.close();
    }

    /**
     * kill the leader while writes are in flight, let a new one be elected,
     * bring the dead node back, and require every acknowledged write to be
     * readable on every node.
     */
    @Test
    public void acknowledgedWritesSurviveLeaderCrash() throws Exception {
        cluster.start();
        RaftNode leader = cluster.awaitLeader(20_000);
        assertNotNull("no leader elected", leader);

        Writers writers = new Writers();
        writers.start();

        writers.awaitAcks(50, 20_000);

        String victim = cluster.leaders().get(0).getNodeId();
        LOGGER.info("killing leader {} with {} acked writes so far", victim, writers.acked.size());
        cluster.stopNode(victim);

        RaftNode newLeader = cluster.awaitLeader(20_000);
        assertNotNull("no leader elected after the crash", newLeader);
        LOGGER.info("new leader is {}", newLeader.getNodeId());

        int acksBeforeCrash = writers.acked.size();
        writers.awaitAcks(acksBeforeCrash + 50, 20_000);

        writers.stop();
        LOGGER.info("writers stopped: {} acked, {} failed", writers.acked.size(), writers.failed.get());

        cluster.restartNode(victim);
        assertAllAckedWritesReadable(writers.acked);
    }

    /*
    *  if all the nodes restart still the acknowledged wrties rae readable
    */
    @Test
    public void acknowledgedWritesSurviveFullClusterRestart() throws Exception {
        cluster.start();
        assertNotNull(cluster.awaitLeader(20_000));

        Writers writers = new Writers();
        writers.start();
        writers.awaitAcks(200, 30_000);
        writers.stop();
        LOGGER.info("acked {} writes before killing the cluster", writers.acked.size());

        for (SocketClusterHarness.Node n : new ArrayList<>(cluster.runningNodes())) {
            cluster.stopNode(n.nodeId);
        }
        // Caches are gone. Anything that comes back came off disk.
        for (SocketClusterHarness.Node n : cluster.nodes()) {
            cluster.restartNode(n.nodeId);
        }

        assertNotNull("cluster did not re-elect after full restart", cluster.awaitLeader(20_000));
        assertAllAckedWritesReadable(writers.acked);
    }


    private void assertAllAckedWritesReadable(Set<String> acked) {
        assertTrue("no writes were ever acknowledged -- the test proved nothing", acked.size() > 0);
        LOGGER.info("verifying {} acknowledged writes on {} nodes", acked.size(), cluster.runningNodes().size());

        try {
            cluster.awaitUntil("every node to hold every acknowledged write", CONVERGE_MS,
                    () -> missingKeys(acked).isEmpty());
        } catch (AssertionError e) {
            List<String> missing = missingKeys(acked);
            fail(String.format(
                    "LOST %d of %d acknowledged writes. First few: %s",
                    missing.size(), acked.size(),
                    missing.subList(0, Math.min(10, missing.size()))));
        }
    }

    /** "node2:k-41" for every acked key absent or wrong on some running node. */
    private List<String> missingKeys(Set<String> acked) {
        List<String> missing = new ArrayList<>();
        for (SocketClusterHarness.Node n : cluster.runningNodes()) {
            for (String key : acked) {
                CacheItem item = n.cache.get(key);
                if (item == null || !key.equals(new String(item.getValue()))) {
                    missing.add(n.nodeId + ":" + key);
                }
            }
        }
        return missing;
    }

    private static String setCommand(String key, String value) {
        Command c = new Command(CommandType.SET, key, 0, 0, value.getBytes().length);
        c.setValue(value.getBytes());
        return c.serialize();
    }

    private final class Writers {
        final Set<String>   acked   = ConcurrentHashMap.newKeySet();
        final AtomicInteger failed  = new AtomicInteger();
        final AtomicInteger nextKey = new AtomicInteger();
        final AtomicBoolean running  = new AtomicBoolean(true);
        final List<Thread>  threads  = new ArrayList<>();
        final CountDownLatch progress = new CountDownLatch(1);

        void start() {
            for (int i = 0; i < WRITERS; i++) {
                Thread t = new Thread(this::loop, "writer-" + i);
                t.setDaemon(true);
                threads.add(t);
                t.start();
            }
        }

        private void loop() {
            while (running.get()) {
                String key = "k-" + nextKey.getAndIncrement();
                try {
                    List<RaftNode> leaders = cluster.leaders();
                    if (leaders.isEmpty()) {           // mid-election, nothing to write to
                        Thread.sleep(20);
                        continue;
                    }
                    leaders.get(0)
                           .propose(setCommand(key, key))
                           .get(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    // ONLY here. The future returned normally, so the client
                    // would have seen STORED, so this key is now a promise.
                    acked.add(key);
                    progress.countDown();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (Exception e) {
                    // Not-leader, timeout, WAL failure. All legal outcomes for an
                    // in-flight write during a crash. Not recorded, not asserted on.
                    failed.incrementAndGet();
                }
            }
        }

        void awaitAcks(int target, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            while (acked.size() < target) {
                if (System.currentTimeMillis() > deadline) {
                    fail(String.format("only %d of %d writes acked within %dms (%d failures)",
                                       acked.size(), target, timeoutMs, failed.get()));
                }
                Thread.sleep(50);
            }
        }

        void stop() throws InterruptedException {
            running.set(false);
            for (Thread t : threads) t.join(10_000);
        }
    }
}
