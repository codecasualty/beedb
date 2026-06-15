package com.memcache.raft;

import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
import com.memcache.command.Command;
import com.memcache.command.CommandType;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
public class RaftClusterTest {

    private InMemoryRaftTransport raftTransport;
    private List<RaftNode> raftNodesList;
    private Map<RaftNode, String> nodeAddresss = new LinkedHashMap<>();
    private Logger LOGGER  = LoggerFactory.getLogger(RaftClusterTest.class.getName());

    @Before
    public void setUp() throws InterruptedException{
        raftNodesList = new ArrayList<>();
        raftTransport = new InMemoryRaftTransport();
        RaftNode raftNode1 = new RaftNode(Arrays.asList( "localhost:11212", "localhost:11213"), "node1", new Cache(), raftTransport);
        RaftNode raftNode2 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11213"), "node2", new Cache(), raftTransport);
        RaftNode raftNode3 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11212"), "node3", new Cache(), raftTransport);
        nodeAddresss.put(raftNode1, "localhost:11211");
        nodeAddresss.put(raftNode2, "localhost:11212");
        nodeAddresss.put(raftNode3, "localhost:11213");
        raftTransport.addRaftNode("localhost:11211", raftNode1);
        raftTransport.addRaftNode("localhost:11212", raftNode2);
        raftTransport.addRaftNode("localhost:11213", raftNode3);
        raftNodesList.add(raftNode1);
        raftNodesList.add(raftNode2);
        raftNodesList.add(raftNode3);
        raftNode1.start();
        raftNode2.start();
        raftNode3.start();
        // election timer is of 300 ms max so by this time we must have an elected elected so lets take some buffer and wait for 500 ms
        Thread.sleep(500);
    }

    @After
    public void tearDown(){
        for(RaftNode node : raftNodesList){
            node.stop();
        }
        raftNodesList.clear();
        raftTransport = null;
        nodeAddresss.clear();
    }

    private RaftNode findLeader() throws InterruptedException{
        int count = 0;
        RaftNode leaderNode = null;
        while(leaderNode == null){
            Thread.sleep(100);
            for(RaftNode node : raftNodesList){
                if(node.getRole() ==  NodeRole.LEADER){
                    count++;
                    leaderNode = node;
                }
            }
        }
        assertEquals(1, count);
        assertNotNull(leaderNode);
        return leaderNode;
    }
    @Test
    public void shouldElectExactlyOneLeader() throws InterruptedException{

        // up to this point we must have 3 raft nodes and one of them must be leader.
        // int count = 0;
        // for(RaftNode node : raftNodesList){
        //     if(node.getRole() == NodeRole.LEADER)count++;
        // }
        // assertEquals(1, count); 
        RaftNode leaderNode = findLeader();
        assertNotNull(leaderNode);

    }

    @Test
    public void shouldReplicateEntryToAllNodes() throws InterruptedException,ExecutionException, TimeoutException{
        // we will have to make sure entries are replicated to all the nodes
        // for that first we find out leader node and send set command to that node
        // then we wait for some time so that replication works in background and 
        // then query any follower node to check if entry is replicted the response should be stored

        RaftNode leaderNode = findLeader();

        Command command = new Command(CommandType.SET, "Foo", 0, 0, 3);
        command.setValue("bar".getBytes());

        // we have pushed entry in leader 
        CompletableFuture<String> future = leaderNode.propose(command.serialize());
        // now wait for replication , lets wait for 100 ms , because after 50 ms hearbeat are send and keeping and headbuffer of 50ms
        // should be enough for replication to complete
        String responseString = future.get(100, TimeUnit.MILLISECONDS);
        LOGGER.info("----------------------------------------");
        LOGGER.info("Response from leader about storing reponse is {} ",responseString);
        LOGGER.info("----------------------------------------");

        assertEquals( "STORED\r\n" , responseString);
        // adding sleep of 100 ms so that entries are applied in cache as well
        Thread.sleep(100);
        // we dont need to create command to get data from other nodes, we can just query their caches, use get()
        LOGGER.info("----------------------------------------");
        for(RaftNode node: raftNodesList){
            if(node == leaderNode) continue;
            CacheItem item = node.get("Foo");
            assertNotNull(item);
            String response = new String(item.getValue(), StandardCharsets.UTF_8);
            LOGGER.info(" respones from peers {} for getting a key Foo is {} ", node.getNodeId() , response);
            assertEquals("bar", response);
            

        }
        LOGGER.info("----------------------------------------");


    }

    @Test
    public void shouldNotAcceptWriteOnFollower() throws InterruptedException,ExecutionException, TimeoutException{
        // when we send entries on follower, it should not be accepted
        RaftNode leaderNode = findLeader();

        Command command = new Command(CommandType.SET, "Hello", 0, 0, 3);
        command.setValue("world".getBytes());

        // push this entry to followers now
        // List<CompletableFuture<String>> futures = new ArrayList<>();
        for(RaftNode node : raftNodesList){
            if(node.getRole() == NodeRole.FOLLOWER){
                CompletableFuture<String> future = node.propose(command.serialize());
                try{
                    future.get(100, TimeUnit.MILLISECONDS);
                }catch(Exception e){
        
                    assertTrue(e.getCause() instanceof IllegalStateException);
                    assertTrue(future.isCompletedExceptionally());
                    assertEquals("Not leader: "+leaderNode.getNodeId(), e.getCause().getMessage());
        
                }
            }
        }
        
    }

    @Test
    public void shouldElectNewLeaderAfterLeaderRemoved() throws InterruptedException,ExecutionException, TimeoutException{
        RaftNode leaderNode = findLeader();
        LOGGER.info("----------------------------------------");

        LOGGER.info("OUR LEADER IS {} ", leaderNode.getNodeId());
        LOGGER.info("----------------------------------------");

        String leaderAddress = nodeAddresss.get(leaderNode);

        // now we will remove leader node from raft nodes list
        raftNodesList.remove(leaderNode);
        raftTransport.removeRaftNode(leaderAddress);
        leaderNode.stop();
        // wait for some time so that election timer can run and elect new leader
        // 300ms for election timeout and 200ms as extra buffer
        Thread.sleep(500);

        RaftNode newLeaderNode = findLeader();
        LOGGER.info("----------------------------------------");

        LOGGER.info("NEW LEADER IS {} ", newLeaderNode.getNodeId());
        LOGGER.info("----------------------------------------");

        assertNotEquals(leaderNode, newLeaderNode);
        assertNotEquals(leaderAddress, nodeAddresss.get(newLeaderNode));



    }
    
}
