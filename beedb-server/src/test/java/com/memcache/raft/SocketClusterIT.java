package com.memcache.raft;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.command.Command;
import com.memcache.command.CommandType;

/**
 * Integration tests over REAL loopback sockets.
 *
 * Named *IT so maven-failsafe runs it in `verify`, not maven-surefire in `test`.
 * That keeps `mvn test` at ~56s for the inner loop; CI runs `mvn verify`.
 */
public class SocketClusterIT {

    @Rule public TemporaryFolder folder = new TemporaryFolder();
    Logger LOGGER = LoggerFactory.getLogger(SocketClusterIT.class.getName());
    private SocketClusterHarness cluster;

    @Before
    public void setUp() throws Exception {
        cluster = new SocketClusterHarness(3, folder.getRoot().toPath());
    }

    @After
    public void tearDown() {
        if (cluster != null) cluster.close();
    }

    private static String setCommand(String key, String value) {
        Command c = new Command(CommandType.SET, key, 0, 0, value.getBytes().length);
        c.setValue(value.getBytes());
        return c.serialize();
    }

    @Test
    public void writeShouldReplicateAndApplyOnEveryNode() throws Exception {
        cluster.start();
        RaftNode leader = cluster.awaitLeader(20_000);
        assertNotNull(leader);

        String result = leader.propose(setCommand("colour", "amber")).get(5, TimeUnit.SECONDS);
        assertEquals("STORED\r\n", result);

        // Commit is a LOG guarantee; applied is a STATE MACHINE guarantee.
        // Assert on applied
        cluster.awaitUntil("all nodes to apply the write", 10_000,
                () -> cluster.nodes().stream().allMatch(
                        n -> "amber".equals(new String(n.cache.get("colour").getValue()))));

        for (SocketClusterHarness.Node n : cluster.nodes()) {
            assertEquals("amber", new String(n.cache.get("colour").getValue()));
        }
    }

    @Test
    public void everyRpcTypeShouldSurviveARealSocket() throws Exception {
        cluster.withSnapshot(5,5);
        cluster.start();
        // request votes is proven if we have leader 
        RaftNode leader = cluster.awaitLeader(20_000);
        LOGGER.info("leader is {} ", leader.getNodeId());
        Map<String, String> stats = cluster.stats(leader.getNodeId());
        String beforeSleepNodeId = stats.get("raft_node_id");
        String beforeSleepTerm = stats.get("raft_term");
        LOGGER.info("fetched ndoe id is {} ", beforeSleepNodeId);
        assertNotNull(beforeSleepNodeId);
        assertNotNull(beforeSleepTerm);

        Thread.sleep(3000);
        RaftNode updatedLeader = cluster.awaitLeader(20_000);
        assertEquals(beforeSleepNodeId, updatedLeader.getNodeId());
        assertEquals(beforeSleepTerm, updatedLeader.getTerm()+"");

        RaftNode follower = cluster.awaitFollower(20_000);
        int followerNodeLastAppliedBeforeRestart = Integer.parseInt(cluster.stats(follower.getNodeId()).get("raft_last_applied"));
        cluster.stopNode(follower.getNodeId());
        Thread.sleep(1000);

        RaftNode newLeader = cluster.awaitLeader(20_000);
        for(int i = 1;i <= 10;i++){
            newLeader.propose(setCommand("key"+i, "value"+i));
        }
        cluster.awaitUntil("all running nodes to hold key10", 10_000,
        () -> cluster.allHaveKey("key10"));
        int leaderLastApplied = Integer.parseInt(cluster.stats(leader.getNodeId()).get("raft_last_applied"));

        cluster.restartNode(follower.getNodeId());
        assertTrue("follower must have missed latest entries ", leaderLastApplied > followerNodeLastAppliedBeforeRestart);

        cluster.awaitUntil("all running nodes to hold key10", 10_000,
        () -> cluster.allHaveKey("key10"));
        
        for(int i = 1;i <= 10;i++){
            for (SocketClusterHarness.Node n : cluster.nodes()) {
                assertEquals("value"+i, new String(n.cache.get("key"+i).getValue()));
            }
            
        }

    }

    @Test
    public void followerShouldCatchUpAfterRejoin() throws Exception {
        cluster.start();
        RaftNode leader = cluster.awaitLeader(20_000);
        LOGGER.info("leader is {} ", leader.getNodeId());
        RaftNode follower = cluster.awaitFollower(20_000);
        cluster.stopNode(follower.getNodeId());
        Thread.sleep(1000);

        RaftNode newLeader = cluster.awaitLeader(20_000);
        for(int i = 1;i <= 100;i++){
            newLeader.propose(setCommand("key"+i, "value"+i));
        }
        cluster.awaitUntil("all running nodes to hold key100", 10_000,
        () -> cluster.allHaveKey("key100"));

        cluster.restartNode(follower.getNodeId());
        cluster.awaitUntil("all running nodes to hold key100", 10_000,
        () -> cluster.allHaveKey("key100"));

        for(int i = 1;i <= 100;i++){
            for (SocketClusterHarness.Node n : cluster.nodes()) {
                assertEquals("value"+i, new String(n.cache.get("key"+i).getValue()));
            }
            
        }

    }
}
