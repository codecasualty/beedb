package com.memcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.command.CommandType;
import com.memcache.raft.InMemoryRaftTransport;
import com.memcache.raft.NodeRole;
import com.memcache.raft.RaftClusterTest;
import com.memcache.raft.RaftNode;

public class ServerRoutingTest {
    private String stateDir = null;
    private String snapshotDir = null;
    private String tmpDir = null;
    private String walDir = null;
    private Logger LOGGER  = LoggerFactory.getLogger(RaftClusterTest.class.getName());
    private InMemoryRaftTransport raftTransport;
    private List<RaftNode> raftNodesList;
    private Map<RaftNode, String> raftNodeAddress = new LinkedHashMap<>();
    private Cache cache1;
    private Cache cache2;
    private Cache cache3;

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void tearDown(){
        for(RaftNode node : raftNodesList){
            node.stop();
        }
        raftNodesList.clear();
        raftTransport = null;
        raftNodeAddress.clear();
    }

    @Before
    public void setUp() throws InterruptedException, IOException{

        raftNodesList = new ArrayList<>();
        raftTransport = new InMemoryRaftTransport();
        stateDir = folder.newFolder("state").getAbsolutePath();
        snapshotDir = folder.newFolder("snapshots").getAbsolutePath();
        tmpDir = folder.newFolder("tmp").getAbsolutePath();
        folder.newFolder("logs").getAbsoluteFile();
        walDir = folder.newFolder("wal").getAbsolutePath();
        LOGGER.debug("before creating node state dir is {}", stateDir);
        cache1 = new Cache();
        cache2 = new Cache();
        cache3 = new Cache();
        RaftNode raftNode1 = new RaftNode(Arrays.asList( "localhost:11212", "localhost:11213"), "node1", cache1, raftTransport, stateDir, snapshotDir, tmpDir, walDir, 5,5, 150, 300, 100, 200, 1000);
        RaftNode raftNode2 = new RaftNode(Arrays.asList( "localhost:11211", "localhost:11213"), "node2", cache2, raftTransport, stateDir, snapshotDir, tmpDir, walDir, 5,5, 150, 300, 100, 200, 1000);
        RaftNode raftNode3 = new RaftNode(Arrays.asList( "localhost:11211", "localhost:11212"), "node3", cache3, raftTransport, stateDir, snapshotDir, tmpDir, walDir, 5,5, 150, 300, 100, 200, 1000);
        raftNodeAddress.put(raftNode1, "localhost:11211");
        raftNodeAddress.put(raftNode2, "localhost:11212");
        raftNodeAddress.put(raftNode3, "localhost:11213");
        raftTransport.addRaftNode("localhost:11211", raftNode1);
        raftTransport.addRaftNode("localhost:11212", raftNode2);
        raftTransport.addRaftNode("localhost:11213", raftNode3);
        raftNodesList.add(raftNode1);
        raftNodesList.add(raftNode2);
        raftNodesList.add(raftNode3);
        raftNode1.start();
        raftNode2.start();
        raftNode3.start();
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
    public void testingServerRouting() throws Exception{
        
        // election timer is of 300 ms max so by this time we must have an elected elected so lets take some buffer and wait for 500 ms
        Thread.sleep(500);
        Server server = new Server();
        RaftNode leader = findLeader();
        long term = leader.getTerm();
        server.raftNode = leader;
        server.cache = leader.getNodeId().equals("node1") ? cache1 : leader.getNodeId().equals("node2") ? cache2 : cache3;
        int logSize = server.raftNode.raftLogSize();

        Command command = new Command(CommandType.SET, "Foo", 0, 0, 3);
        command.setValue("bar".getBytes());
        byte[] response = server.processRequest(command);
        LOGGER.debug("resposne in server is {}", new String(response));
        String responseText = "STORED\r\n";
        assertEquals(responseText, new String(response));
        assertEquals(logSize + 1, server.raftNode.raftLogSize());
        
        // waiting for entry being applied to cache so that get is successfull 
        // Thread.sleep(1000);
        logSize = server.raftNode.raftLogSize();
        Command command2 = new Command(CommandType.GET, "Foo", 0, 0, -1);
        byte[] response2 = server.processRequest(command2);
        LOGGER.debug("resposne in server is {}", new String(response2));
        String responseText2 = "VALUE Foo 0 3\r\nbar\r\n" + //
                        "END\r\n";
        assertEquals(responseText2, new String(response2));
        assertEquals(logSize, server.raftNode.raftLogSize());

        logSize = server.raftNode.raftLogSize();
        Command command3 = new Command(CommandType.DELETE, "Foo", 0, 0, 0);
        byte[] response3 = server.processRequest(command3);
        LOGGER.debug("resposne in server is {}", new String(response3));
        String responseText3 = "DELETED\r\n";
        assertEquals(responseText3, new String(response3));

        assertEquals(logSize + 1, server.raftNode.raftLogSize());
        logSize = server.raftNode.raftLogSize();
        Command command4 = new Command(CommandType.STATS, null, 0, 0, -1);
        byte[] response4 = server.processRequest(command4);
        LOGGER.debug("resposne in server is {}", new String(response2));
        String responseText4 = "STAT curr_items 0\r\n" + 
                                "STAT raft_node_id "+leader.getNodeId()+"\r\n" + //
                                "STAT raft_role LEADER\r\n" + //
                                "STAT raft_term "+term+"\r\n" + //
                                "STAT raft_commit_index 3\r\n" + //
                                "STAT raft_last_applied 3\r\n" + //
                                "STAT raft_leader_id "+leader.getNodeId()+"\r\n" + //
                                "STAT raft_last_included_index 0\r\n" + //
                                "STAT raft_log_size 3\r\n" + //
                                "END\r\n";
        
        assertEquals(responseText4, new String(response4));
        assertEquals(logSize, server.raftNode.raftLogSize());

    }
}
