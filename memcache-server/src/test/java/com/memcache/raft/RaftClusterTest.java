package com.memcache.raft;

import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
import com.memcache.command.Command;
import com.memcache.command.CommandType;

import org.junit.Test;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
public class RaftClusterTest {

    private InMemoryRaftTransport raftTransport;
    private List<RaftNode> raftNodesList;
    private Logger LOGGER  = LoggerFactory.getLogger(RaftClusterTest.class.getName());

    @Before
    public void setUp() throws InterruptedException{
        raftNodesList = new ArrayList<>();
        raftTransport = new InMemoryRaftTransport();
        RaftNode raftNode1 = new RaftNode(Arrays.asList( "localhost:11212", "localhost:11213"), "node1", new Cache(), raftTransport);
        RaftNode raftNode2 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11213"), "node2", new Cache(), raftTransport);
        RaftNode raftNode3 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11212"), "node3", new Cache(), raftTransport);
        raftTransport.addRaftNode("localhost:11211", raftNode1);
        raftTransport.addRaftNode("localhost:11212", raftNode2);
        raftTransport.addRaftNode("localhost:11213", raftNode3);
        raftNodesList.add(raftNode1);
        raftNodesList.add(raftNode2);
        raftNodesList.add(raftNode3);
        // election timer is of 300 ms max so by this time we must have an elected elected so lets take some buffer and wait for 500 ms
        Thread.sleep(500);
    }

    @Test
    public void shouldElectExactlyOneLeader(){

        // up to this point we must have 3 raft nodes and one of them must be leader.
        int count = 0;
        for(RaftNode node : raftNodesList){
            if(node.getRole() == NodeRole.LEADER)count++;
        }
        assert count == 1;

    }

    @Test
    public void shouldReplicateEntryToAllNodes() throws InterruptedException,ExecutionException, TimeoutException{
        // we will have to make sure entries are replicated to all the nodes
        // for that first we find out leader node and send set command to that node
        // then we wait for some time so that replication works in background and 
        // then query any follower node to check if entry is replicted the response should be stored

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
        assert count == 1;
        assert leaderNode != null;

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

        assert responseString.equals("STORED\r\n");

        // we dont need to create command to get data from other nodes, we can just query their caches, use get()
        boolean replicated =false;
        LOGGER.info("----------------------------------------");
        for(RaftNode node: raftNodesList){
            if(node == leaderNode) continue;
            CacheItem item = node.get("Foo");
            if(item != null){
                String response = new String(item.getValue(), StandardCharsets.UTF_8);
                LOGGER.info(" respones from peers {} for getting a key Foo is {} ", node.getNodeId() , response);
                if(response.equals("bar"))replicated = true;
            }

        }
        LOGGER.info("----------------------------------------");

        // atleast one node should have entry as bar
        assert replicated == true;

    }
    
}
