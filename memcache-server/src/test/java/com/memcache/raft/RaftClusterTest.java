package com.memcache.raft;

import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
import com.memcache.command.Command;
import com.memcache.command.CommandType;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.wal.WalService;

import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertNotEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
public class RaftClusterTest {

    private InMemoryRaftTransport raftTransport;
    private List<RaftNode> raftNodesList;
    private Map<RaftNode, String> raftNodeAddress = new LinkedHashMap<>();
    private Logger LOGGER  = LoggerFactory.getLogger(RaftClusterTest.class.getName());
    private String stateDir = null;
    private String snapshotDir = null;
    private String tmpDir = null;
    private String walDir = null;
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    @Before
    public void setUp() throws InterruptedException, IOException{
        raftNodesList = new ArrayList<>();
        raftTransport = new InMemoryRaftTransport();
        stateDir = folder.newFolder("state").getAbsolutePath();
        snapshotDir = folder.newFolder("snapshots").getAbsolutePath();
        tmpDir = folder.newFolder("tmp").getAbsolutePath();
        folder.newFolder("logs").getAbsoluteFile();
        walDir = folder.newFolder("wal").getAbsolutePath();
        LOGGER.info("before creating node state dir is {}", stateDir);
        RaftNode raftNode1 = new RaftNode(Arrays.asList( "localhost:11212", "localhost:11213"), "node1", new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);
        RaftNode raftNode2 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11213"), "node2", new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);
        RaftNode raftNode3 = new RaftNode(Arrays.asList("localhost:11211", "localhost:11212"), "node3", new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);
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
        raftNodeAddress.clear();
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
    public void shouldElectExactlyOneLeader(){

        // up to this point we must have 3 raft nodes and one of them must be leader.
        // int count = 0;
        // for(RaftNode node : raftNodesList){
        //     if(node.getRole() == NodeRole.LEADER)count++;
        // }
        // assertEquals(1, count); 
        try{
            RaftNode leaderNode = findLeader();
            assertNotNull(leaderNode);
        }catch(Exception e){
            // dont throws interrupted exception
            e.printStackTrace();
            // fail("Test failed, please check stack trace");
        }

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
        Thread.sleep(500);
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
                    assertEquals("Not Leader: "+leaderNode.getNodeId(), e.getCause().getMessage());
        
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

        String leaderAddress = raftNodeAddress.get(leaderNode);

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
        assertNotEquals(leaderAddress, raftNodeAddress.get(newLeaderNode));



    }

    @Test
    public void shouldMaintainDataAfterReelection() throws InterruptedException,ExecutionException, TimeoutException{
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
        LOGGER.info("Response from leader before killing leader is {} ",responseString);
        LOGGER.info("----------------------------------------");

        assertEquals( "STORED\r\n" , responseString);
        // String expectedValue = new String(leaderNode.get(command.getKey()).getValue());
        // adding sleep of 100 ms so that entries are applied in cache as well
        Thread.sleep(100);

        // now we stop the leader and wait for some time so that new leader can be elected and check if our prevoius keys is still present or not
        raftNodesList.remove(leaderNode);
        raftTransport.removeRaftNode(raftNodeAddress.get(leaderNode));
        leaderNode.stop();

        Thread.sleep(500);

        // this is our new leader 
        leaderNode = findLeader();

        // now check if still have that previous inserted entry
        for(RaftNode node : raftNodesList){
            CacheItem item = node.get(command.getKey());
            assertNotNull(item);
            String storedValueInFollower = new String(item.getValue());
            LOGGER.info("----------------------------------------");
            LOGGER.info("Response from new leader is after eleciton {} ",storedValueInFollower);
            LOGGER.info("----------------------------------------");
            assertEquals("bar", storedValueInFollower);
            
        }


    }

    @Test
    public void shouldNotCommitWithoutMajority() throws InterruptedException, ExecutionException{
        RaftNode leader = findLeader();

        // remove the followers i.e. stop and remove from transport
        List<RaftNode> toRemove = new ArrayList<>();
        for(RaftNode node : raftNodesList){
            if(node == leader) continue;
            toRemove.add(node);

        }
        
        for(RaftNode node : toRemove){
            raftNodesList.remove(node);
            raftTransport.removeRaftNode(raftNodeAddress.get(node));
            node.stop();
        }

        // now chekc if entry get committed in leader
        Command command = new Command(CommandType.SET, "Foo", 0, 0, 3);
        command.setValue("bar".getBytes());

        CompletableFuture<String> result = leader.propose(command.serialize());

        try{
            result.get(1000 , TimeUnit.MILLISECONDS);
            fail("Entry shouldn't get committed without majority");
        }catch(TimeoutException e){
            LOGGER.info("received a timeout exeception");
        }
    }
    
    @Test
    public void shouldFollowerCatchUpAfterRejoin() throws InterruptedException{

        // here first we will figure out leader and then remove one follower from transport layer,
        // we are not going to kill it , we are just partitioning our network. 
        // and push some x entries in leader and let it replicate on our cluster 
        // and then bring back our follower which we thrown out of cluster and wait for replicatoin
        // to catch up and then we will check our cluster is workign as expected.

        RaftNode leader = findLeader();
        RaftNode nodeToRemove = null;
        for(RaftNode node : raftNodesList){
            if(node == leader)continue;
            nodeToRemove = node;
            break;
        }
        assertNotNull(nodeToRemove);

        // removing that node from our transport , not killing not calling stop
        String addressNodeToRemoveString = raftNodeAddress.get(nodeToRemove);
        raftTransport.removeRaftNode(addressNodeToRemoveString);
        raftNodeAddress.remove(nodeToRemove);
        // will now push some entries in our funcitonal cluster
        // and wait for them to get replicated
        List<CompletableFuture<String>> futures = new ArrayList<>();
        for(int i = 1;i <= 10;i++){
            Command command = new Command(CommandType.SET, "key"+i, 0, 0, 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> result = leader.propose(command.serialize());
            futures.add(result);
        }

        for(CompletableFuture<String> future : futures){
            try {
                String response = future.get(500 , TimeUnit.MILLISECONDS);
                assertEquals("STORED\r\n", response);
            } catch (Exception e) {
                LOGGER.info("entry is not committed in leader, waiting for majority");
                fail("Entry is not committed in leader ........ FAILED");
            }
        }

        // now we bring back our follower, which was thrown out of cluster
        raftTransport.addRaftNode(addressNodeToRemoveString , nodeToRemove );
        raftNodeAddress.put(nodeToRemove , addressNodeToRemoveString);
        // now we will wait for 5000 ms because we hvae pushed 10 entires , so those entries should get replicated to follower and we dont want to 
        // query that node , because it may still be catching up with the leader. so we are waiting generously 
        // the reason we have such big sleep is , because there will be exchange of two snapshots and few entries as well and we dont want to query follower
        // too early
        LOGGER.info("waiting for 5 seconds before querying follower node {} ", nodeToRemove.getNodeId());
        Thread.sleep(5000);

        for(int i = 10;i >= 1 ;i--){
            CacheItem item = nodeToRemove.get("key"+i);
            CacheItem leaderItem = leader.get("key"+i);
            LOGGER.info("asking leader {} for {} and value is {} ",leader.getNodeId(), "key"+i, new String(leaderItem.getValue()));
            LOGGER.info("asking follower {} for {} and value is {} ",nodeToRemove.getNodeId(), "key"+i, new String(item.getValue()));
            assertNotNull(item);
            String value = new String(item.getValue());
            LOGGER.info("vlaue stored at follower is {}", value);
            assertEquals("value"+i , value);
        }


    }
    @Test
    public void shouldIncrementTermAfterReelection() throws InterruptedException{
        // first we find out leader and its current term that term will be accepted by all the followers
        // then we kill the leader and wait for new leader , 
        // once we have new leader we check whether the new term > previous term this is important because without that
        // our complete foundation of leader based replication fails

        RaftNode leader = findLeader();
        long leaderTerm = leader.getTerm();
        String leaderAddress = raftNodeAddress.get(leader);
        // nowe we will kill the leadernode

        raftNodesList.remove(leader);
        raftTransport.removeRaftNode(leaderAddress);
        leader.stop();


        // now wait for reelection to complete
        Thread.sleep(500);

        RaftNode newLeader = findLeader();
        assertNotNull(newLeader);

        long newTerm = newLeader.getTerm();

        assertTrue( "Expected newterm "+newTerm+" to be greater than previous term "+leaderTerm , newTerm > leaderTerm );

        
    }

    @Test
    public void shouldHandleConcurrentWrites() throws InterruptedException{
        // first we find out leader and propose 10 entries/writes to leader without waitingg or sleeping
        // collect all futures in list and then wait for them to resolve, i.e. to get stored\r\n response
        // note this does not mean its applied to cache, that is done by another apply committed entry threads
        // so we wait for atleast 300-400 ms and then check on all the followers whether the entries are applied correctly 
        // or not.
        // the reason behind waiting at least 300ms is, because replication loop has sleep of 100 ms
        // so we account for network delay and that would be total of 300ms

        RaftNode leader = findLeader();

        List<CompletableFuture<String>> futures = new ArrayList<>();
        for(int i = 0;i < 10;i++){
            Command command = new Command(CommandType.SET , "key"+i,0, 0 , 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> result = leader.propose(command.serialize());
            futures.add(result);
        }

        for(CompletableFuture<String> future : futures){
            try{
                String response = future.get(300, TimeUnit.MILLISECONDS);
                assertEquals("STORED\r\n", response);
            }catch(Exception e){
                fail("Test failed raise exception");
            }
        }

        // now we wait for entry to get applied to caches of followers
        Thread.sleep(300);
        for(RaftNode node : raftNodesList){
            if(node == leader)continue;
            LOGGER.info("Checking whether the node {} ", raftNodeAddress.get(node));
            for(int i = 0;i < 10;i++){
                CacheItem item = node.get("key"+i);
                assertNotNull(item);
                String value = new String(item.getValue());
                assertEquals("value"+i, value);
                LOGGER.info("value stored for key {} is {} ", ("key"+i) , "value"+i);
            }
        }
        
    }

    @Test
    public void shouldNotGrantVoteToStaleCandidate() throws InterruptedException{
        // the idea is to first find leader, then remove any one follower and then insert data in cluster, 
        // then remove leader and attach that removed follower in our cluster, now check the next leader shouldn't be this newly attached follower
        // as its stale
        RaftNode leader = findLeader();
        RaftNode follower = null;
        for(RaftNode node : raftNodesList){
            if(node == leader) continue;
            raftTransport.removeRaftNode(raftNodeAddress.get(node));
            follower = node;
            break;
        }

        assertNotNull(follower);
        Command command = new Command(CommandType.SET, "Foo", 0, 0, 3);
        command.setValue(("bar").getBytes());

        CompletableFuture<String> response = leader.propose(command.serialize());
        try{
            String result = response.get(100, TimeUnit.MILLISECONDS);
            assertEquals("STORED\r\n" , result);
        }catch(Exception e){
            e.printStackTrace();
            fail("value is not stored in cluster -- there is no point continuing further");
        }

        raftTransport.removeRaftNode(raftNodeAddress.get(leader));
        raftNodesList.remove(leader);
        leader.stop();

        raftTransport.addRaftNode(raftNodeAddress.get(follower), follower);
        // waiting for election process to complete
        Thread.sleep(500);

        RaftNode newLeader = findLeader();
        assertNotEquals(follower.getNodeId(), newLeader.getNodeId());
    }


    @Test
    public void shouldNotGetLeaderIdDuringElection() throws InterruptedException{
        RaftNode leader = findLeader();
        raftTransport.removeRaftNode(raftNodeAddress.get(leader));
        raftNodesList.remove(leader);
        leader.stop();
        Command command = new Command(CommandType.SET , "Foo", 0, 0 ,3);
        command.setValue("bar".getBytes());
        String serializedString = command.serialize();
        for(RaftNode node : raftNodesList){
            CompletableFuture<String> future = node.propose(serializedString);
            try{
                future.get();
                LOGGER.info("This must have been exception but its not exception ");
                fail("Test failed, Expected Exception :- Election in progress");

            }catch(Exception e){
                LOGGER.info("exceptin is {}" , e.getCause());
                assertTrue(e.getCause() instanceof IllegalStateException);
            }

        }

    }

    @Test
    public void shouldReadWalAfterRestart() throws InterruptedException{

        RaftNode leader = findLeader();
        List<String> peerAddress = leader.getPeerAddressList();
        Command command = new Command(CommandType.SET, "Foo", 0, 0, 4);
        command.setValue("code".getBytes());
        CompletableFuture<String> future = leader.propose(command.serialize());
        try{
            future.get(100, TimeUnit.SECONDS);
        }catch(Exception e){
            fail("Test failed, expected value to be stored in cluster");
        }
        RaftLog raftlog = leader.getLog();
        raftTransport.removeRaftNode(raftNodeAddress.get(leader));
        raftNodesList.remove(leader);
        leader.stop();
        Thread.sleep(500);
        LOGGER.info("--------------------leader is stopped------------------");
        // now we will spawn a new node and put it in leaders position 
        RaftNode newNode = new RaftNode(peerAddress, leader.getNodeId(), new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);
        // raftNodeAddress.put(newNode, "localhost:11211");
        // raftTransport.addRaftNode(raftNodeAddress.get(newNode), newNode);
        // raftNodesList.add(newNode);
        // newNode.start();
        LOGGER.info("--------------------new node is reading from wal ------------------");

        Thread.sleep(500);
        // now check if we are able to read the wal and state from the stored system
        RaftLog raftlogNewNode = newNode.getLog();
        assertEquals(raftlog.lastIndex(), raftlogNewNode.lastIndex());
        assertEquals(raftlog.lastTerm(), raftlogNewNode.lastTerm());
        assertEquals(raftlog.size(), raftlogNewNode.size());
        for(int i = raftlog.getLastIncludedIndex(); i < raftlog.lastIndex() ;i++){
            LOGGER.info("raftlog.get({}) is {} and raftlogNewNode.get({}) is {}", i, raftlog.get(i), i, raftlogNewNode.get(i));
            assertEquals(raftlog.get(i) , raftlogNewNode.get(i));
        }
    }

    @Test
    public void shouldCompactLog() throws InterruptedException{
        RaftNode leader = findLeader();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        // 5 entries will be compacted and one extra entry will be appended in wal
        for(int i = 0;i < 6;i++){
            Command command = new Command(CommandType.SET , "key"+i,0, 0 , 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> result = leader.propose(command.serialize());
            futures.add(result);
        }
        for(CompletableFuture<String> future : futures){
            try {
                String response = future.get(3, TimeUnit.SECONDS);
                assertEquals("STORED\r\n", response);
            } catch (Exception e) {
                fail("Test failed raise exception");
            }
        }

        Thread.sleep(500);
        // List<LogEntry> raftlog = leader.walService.replayFrom(0);
        RaftLog raft = leader.getLog();
        List<LogEntry> raftlog = raft.getFrom(raft.getFirstIndex());
        LOGGER.info("raftlog size is {} ", raftlog.size());
        LOGGER.info("raftlog first index is {} ", raftlog.get(0).getIndex());
        LOGGER.info("raftlog last index is {} ", raftlog.get(raftlog.size() - 1).getIndex());
        LOGGER.info("last included index is {} ", leader.getLog().getLastIncludedIndex());
        List<LogEntry> walEntries = leader.walService.replayFrom(leader.getLog().getLastIncludedIndex());
        for(LogEntry logEntry : raftlog){
            LOGGER.info("log entry after compaction is {}", logEntry);
        }
        for(LogEntry logEntry : walEntries){
            LOGGER.info("wal service entry after compaction is {}", logEntry);
        }
        assertEquals(3, leader.getLog().size());
        assertEquals(5, leader.getLog().getFirstIndex());
        assertEquals(5, leader.getLog().getLastIncludedIndex());
        assertEquals(7, leader.getLog().lastIndex());
        assertEquals(2, walEntries.size());
        assertEquals(6, walEntries.get(0).getIndex());
        assertEquals(7, walEntries.get(1).getIndex());
        // because index 0 is sentenel entry , index 1 will be leader no op entry
        // index 2 - 7 will be keys from key0 index 2, key 1 -> index 3 , key 2 -> index 4 , key 3 -> index 5
        // key 4 -> index 6 , key 5 -> index 7 ,  till key5
        // so when we are doing compaction till index 5, then leader no op, key 0 -> index 2 , key 1 -> index 3 , key 2 -> index 4 , key 3 -> index 5
        // are compacted and we have key 4 -> index 6 , key 5 -> index 7 ,  till key5 in our wal
        // and key 3 -> index 5 key 4 -> index 6 key 5 -> index 7 are in our raftlog
    }

    @Test
    public void shouldNotMissEntriesAfterCompactionInserionInWal() throws InterruptedException{

        RaftNode leader = findLeader();
        List<CompletableFuture<String>> futures = new ArrayList<>();
        // 5 entries will be compacted and one extra entry will be appended in wal
        for(int i = 0;i < 6;i++){
            Command command = new Command(CommandType.SET , "key"+i,0, 0 , 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> result = leader.propose(command.serialize());
            futures.add(result);
        }
        for(CompletableFuture<String> future : futures){
            try {
                String response = future.get(3, TimeUnit.SECONDS);
                assertEquals("STORED\r\n", response);
            } catch (Exception e) {
                fail("Test failed raise exception");
            }
        }

        Thread.sleep(500);
        // now we will insert some entries in our wal
        for(int i = 6;i < 8;i++){
            Command command = new Command(CommandType.SET , "key"+i,0, 0 , 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> result = leader.propose(command.serialize());
            futures.add(result);
        }
        for(CompletableFuture<String> future : futures){
            try {
                String response = future.get(3, TimeUnit.SECONDS);
                assertEquals("STORED\r\n", response);
            } catch (Exception e) {
                fail("Test failed raise exception");
            }
        }

        // now all these entries will be in our raftlog and wal 
        Thread.sleep(500);
        List<LogEntry> raftlog = leader.getLog().getFrom(leader.getLog().getFirstIndex());
        LOGGER.info("raftlog size is {} ", raftlog.size());
        LOGGER.info("raftlog first index is {} ", raftlog.get(0).getIndex());
        LOGGER.info("raftlog last index is {} ", raftlog.get(raftlog.size() - 1).getIndex());
        LOGGER.info("first index in raftlog is {} ", leader.getLog().getFirstIndex());
        LOGGER.info("last included index is {} ", leader.getLog().getLastIncludedIndex());
        assertEquals(5, raftlog.size());
        assertEquals(5, raftlog.get(0).getIndex());
        assertEquals(6, raftlog.get(1).getIndex());
        assertEquals(7, raftlog.get(2).getIndex());
        assertEquals(8, raftlog.get(3).getIndex());
        assertEquals(9, raftlog.get(4).getIndex());
        List<LogEntry> walEntries = leader.walService.replayFrom(leader.getLog().getLastIncludedIndex());
        for(LogEntry logEntry : walEntries){
            LOGGER.info("wal service entry after compaction is {}", logEntry);
        }
        assertEquals(4, walEntries.size()); // because we have inserted 2 entries in wal
        assertEquals(6, walEntries.get(0).getIndex());
        assertEquals(7, walEntries.get(1).getIndex());
        assertEquals(8, walEntries.get(2).getIndex());
        assertEquals(9, walEntries.get(3).getIndex());
    }

    @Test
    public void shouldRestoreFromSnapshotAfterRestart() throws InterruptedException{

        RaftNode leader = findLeader();
        List<CompletableFuture<String>> list = new ArrayList<>();
        for(int i = 1;i <= 7;i++){
            Command command = new Command(CommandType.SET, "key"+i, 0, 0, 6);
            command.setValue(("value"+i).getBytes());
            CompletableFuture<String> future = leader.propose(command.serialize());
            list.add(future);
        }
        Thread.sleep(500);
        // waiting for replication to complete
        for(CompletableFuture<String> future : list){
            try{
                String response = future.get(100, TimeUnit.SECONDS);
                assertEquals("STORED\r\n", response);
            }catch(Exception e){
                fail("Test failed, expected value to be stored in cluster");
            }
        }
        // because if we only stop leader, followers node do have address of leader and they will start eleciotn and start sending no op entry to leader(so called leader node )
        // and our so called leader will replicate it , because our handle append entries will start as independent rpc threads and that would start inserting entries in our log
        // therefore we are stopping all nodes 
        for(RaftNode node : new ArrayList<>(raftNodesList)){
            raftTransport.removeRaftNode(raftNodeAddress.get(node));
            raftNodesList.remove(node);
            raftNodeAddress.remove(node);
            node.stop();
        }
        Thread.sleep(500);
        // now we will spawn a new node and put it in leaders position 
        RaftNode raftNode1 = new RaftNode(Arrays.asList( "localhost:11212", "localhost:11213"), "node1", new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);

        // raftNode1.start();
        LOGGER.info("--------------------new node is reading from wal ------------------");
        // now check if we are able to read the wal and state from the stored system
        RaftLog raftlogNewNode = raftNode1.getLog();
        RaftLog leaderRaftlog = leader.getLog();
        LOGGER.debug("printing leader raft log {} ", leaderRaftlog);
        LOGGER.debug("printing new raft node log {} ", raftlogNewNode);
        // now we can compare the logs size and last included index
        assertEquals(leaderRaftlog.lastIndex(), raftlogNewNode.lastIndex());
        assertEquals(leaderRaftlog.lastTerm(), raftlogNewNode.lastTerm());
        assertEquals(leaderRaftlog.size(), raftlogNewNode.size());
        for(int i = leaderRaftlog.getLastIncludedIndex(); i <= leaderRaftlog.lastIndex() ;i++){
            LOGGER.info("raftlog.get({}) is {} and raftlogNewNode.get({}) is {}", i, leaderRaftlog.get(i), i, raftlogNewNode.get(i));
            assertEquals(leaderRaftlog.get(i).getTerm() , raftlogNewNode.get(i).getTerm());
            assertEquals(leaderRaftlog.get(i).getIndex() , raftlogNewNode.get(i).getIndex());
        }

        for(int i = 1;i < leaderRaftlog.getLastIncludedIndex();i++){
            CacheItem item = raftNode1.getCache().get("key"+i);
            CacheItem leaderItem = leader.getCache().get("key"+i);
            LOGGER.info("asking leader {} for {} and value is {} ",leader.getNodeId(), "key"+i, new String(leaderItem.getValue()));
            LOGGER.info("asking follower {} for {} and value is {} ",raftNode1.getNodeId(), "key"+i, new String(item.getValue()));
            assertNotNull(item);
            LOGGER.info("vlaue stored for key {} is {} ", ("key"+i) , "value"+i);
            assertEquals(new String(leaderItem.getValue() , StandardCharsets.UTF_8) , new String(item.getValue() , StandardCharsets.UTF_8));
        }

        // now lets check if somehow we restored key5-key7 from raft logs 
        for(int i = leaderRaftlog.getLastIncludedIndex() ; i <= leaderRaftlog.lastIndex() ;i++){
            CacheItem item = raftNode1.getCache().get("key"+i);
            assertNull(item);
        }
    }

    @Test
    public void shouldRestoreTermAndVoteAfterRestart() throws InterruptedException{
        RaftNode leader = findLeader();
        // first we let eleciton settle up and then kill the leader and find out what was saved as term and votedFor
        // and after we start another node with same dirs , we assert if the snapshot is restored correctly with correct votedFor and term
        // after that we generate a RequestVoteRequest with same term 
        // then set some different value of voted for and check if we get a voteGranted response as false , then set with correcte voted for and now we should get voteGranted as true
        // also try sending term as restored term - 1 adn we should get denial because we are behind in term
        for(RaftNode node : new ArrayList<>(raftNodesList)){
            raftTransport.removeRaftNode(raftNodeAddress.get(node));
            raftNodesList.remove(node);
            raftNodeAddress.remove(node);
            node.stop();
        }

        Thread.sleep(500);


        RaftStateManager raftStateManager = new RaftStateManager(stateDir, tmpDir, leader.getNodeId());
        RaftState raftState = raftStateManager.deserialize(leader.getNodeId());
        assertNotNull(raftState);
        assertEquals(leader.getTerm(), raftState.getTerm());
        assertEquals(leader.votedFor(), raftState.getVotedFor());
        // now lets start another node with same dirs and assert if the snapshot is restored correctly with correct votedFor and term
        List<String> peerAddress = leader.getPeerAddressList();
        RaftNode raftNode1 = new RaftNode(peerAddress, leader.getNodeId(), new Cache(), raftTransport, stateDir, snapshotDir, tmpDir, walDir);
        assertEquals(leader.getTerm(), raftNode1.getTerm());
        assertEquals(leader.votedFor(), raftNode1.votedFor());

        RequestVoteRequest request = new RequestVoteRequest(leader.getTerm() - 1, leader.getLog().lastIndex(), leader.getLog().lastTerm(), leader.getNodeId(), "");
        RequestVoteResponse response = raftNode1.handleRequestVote(request);
        assertFalse(response.isVoteGranted());
        request.setTerm(leader.getTerm());
        response = raftNode1.handleRequestVote(request);
        assertTrue(response.isVoteGranted());
        
    }



}
