package com.memcache.raft;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.Arrays;
import com.memcache.cache.CacheItem;
import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.wal.EntryType;
import com.memcache.raft.wal.WalRecord;
import com.memcache.raft.wal.WalService;
import com.memcache.handler.CommandProcessor;
import com.memcache.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
public class RaftNode{
    
    // persistent fields
    private long      currentTerm;
    private String      votedFor;
    private RaftLog  log;

    // volatile fields
    private int      commitIndex;
    private int      lastApplied;
    private String   leaderId;
    private NodeRole role;

    // leader only fields
    private Map<String, PeerState> peers; 

    /*
    Scheduler thread > startElection(), resetElectionTimer()
    RPC handler thread > handleRequestVote(), handleAppendEntries()
    Apply thread > applyCommittedEntries()
     */
    // for timer management
    private final ScheduledExecutorService scheduler;
    private       ScheduledFuture<?>       electionTimeoutFuture;
    
    // for network communication
    private final ExecutorService          rpcExecutor;
    // for applying the log, when we receives response from followers we apply the log and update the nextindex and match index and commit logs as well
    // so for each follower we have a virtual threads whose job is to replication  
    private final ExecutorService          applyExecutor;

    // external dependencies
    private List<String>     peerAddresses;
    private String          nodeId;
    private Cache           cache;

    // to store pending requests
    private final Map<Integer, CompletableFuture<String>> pendingRequests;

    // transport layer to send requests to peers
    private RaftTransport transport;

    // raft snapshotmanager for serializing & deserializing data
    RaftSnapshotManager raftSnapshotManager;
    RaftStateManager    raftStateManager;
    WalService          walService;
    private static final Logger LOGGER = LoggerFactory.getLogger(RaftNode.class);

    // Metrics go to a dedicated "METRICS" logger, not the class logger, so they can be
    // switched on for a benchmark without also enabling this class's DEBUG output.
    // logback.xml has it OFF by default: under load these lines were ~40k/minute,
    // written to the same disk the WAL fsyncs to.
    private static final Logger METRICS = LoggerFactory.getLogger("METRICS");

    // raft snapshot progress variable
    // its marked as volatile because its changed by threads while other threads might read
    // the value from memory and its values may be changed in threads cache/register and may not be reflected in memory
    // easiest way to get happens-before relationship
    private volatile boolean inProgress;
    private          int     snapShotThreshold = 1000;
    private          int     snapShotLimit = 1000;
    private final    int     maxEntriesPerRequest = 50;
    private          int     persistedWalIndex;
    private int minElectionTimeout;
    private int maxElectionTimeout;
    private int heartbeatInterval;
    private int peerRetryBackoffInitialMs;
    private int peerRetryBackoffMaxMs;
    private volatile boolean stopping = false;

    // locks and conditions for waking up replication threads from sleeping heartbeat intervals
    private final ReentrantLock replicationLock = new ReentrantLock();
    private final Condition notNewElements = replicationLock.newCondition();
    
    

    public RaftNode(List<String> peerAddresses, String nodeId, Cache cache, RaftTransport transport,
                    String stateDir, String snapshotDir, String tmpDir, String walDir,
                    int snapShotLimit, int snapShotThreshold, int minElectionTimeout, int maxElectionTimeout, int heartbeatInterval,
                    int peerRetryBackoffInitialMs, int peerRetryBackoffMaxMs
    ) {
        this.peerAddresses = peerAddresses;
        this.nodeId = nodeId;
        this.cache = cache;
        this.log = new RaftLog();
        this.peers = new HashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.applyExecutor = Executors.newSingleThreadExecutor();
        this.role = NodeRole.FOLLOWER;
        this.pendingRequests = new ConcurrentHashMap<>();
        this.transport = transport;
        this.snapShotLimit = snapShotLimit;
        this.snapShotThreshold = snapShotThreshold;
        this.minElectionTimeout = minElectionTimeout;
        this.maxElectionTimeout = maxElectionTimeout;
        this.heartbeatInterval = heartbeatInterval;
        this.peerRetryBackoffInitialMs = peerRetryBackoffInitialMs;
        this.peerRetryBackoffMaxMs = peerRetryBackoffMaxMs;
        MDC.put("nodeId", nodeId);
        this.raftSnapshotManager = new RaftSnapshotManager(snapshotDir, tmpDir, nodeId);
        this.raftStateManager   =  new RaftStateManager(stateDir, tmpDir, nodeId);
        RaftSnapshot raftSnapShot = raftSnapshotManager.deserialize(nodeId);
        RaftState    raftState    = raftStateManager.deserialize(nodeId);
        try{
            this.walService           = new WalService(walDir+"/"+nodeId+"/wal.log", tmpDir+"/"+nodeId);
        }catch(Exception e){
            LOGGER.error("error while creating wal service, no walDir found or some other error", e);
            if(!stopping)
                System.exit(1);
        }
        if(raftSnapShot != null){
            lastApplied = commitIndex = raftSnapShot.getLastAppliedIndex();
            long lastIncludedTerm = raftSnapShot.getLastAppliedTerm();
            this.log = new RaftLog(lastApplied, lastIncludedTerm);
            log.setLastIncludedIndex(lastApplied);
            log.setLastIncludedTerm(lastIncludedTerm);
            this.cache.restoreState(raftSnapShot.getCacheState());
            LOGGER.info("restoring from snapshot , last included index {} last included term {} ", log.getLastIncludedIndex() , log.getLastIncludedTerm());
        }
        if(raftState != null){
            this.currentTerm = raftState.getTerm();
            this.votedFor = raftState.getVotedFor();
        }
        this.log.appendAll(this.walService.replayFrom(lastApplied));
        this.applyExecutor.submit(wrapRunnableWithMdc(this::applyCommitedEntries));
        inProgress = false;
        // note its last index() not last applied, because once our node start fresh then both log last index and last applied are same
        // but when we restart then they will drift, last applied will denote how many entries we have applied to our cache, before restart
        // but last index will denote how many entries in wal are present and now are in our raftlog as well, and can be applied to our cache
        this.persistedWalIndex = log.lastIndex();

        LOGGER.info("in raft node constructor lastApplied {} commit index {}  current term {} voted for {} ", lastApplied, commitIndex, currentTerm, votedFor);
    }
    
    public synchronized void start(){
        //  a timer to wait for heartbeat from leader
        resetElectionTimer();
    }

    // avoiding synchronized keyword because we dont want to block the main thread
    // because while we are holding lock on this , we are doing synchronized on reqeustvoteforpeers and java's synchronized are reentrant in nature
    // that means , if we are holding lock on this , we can call reqeustvoteforpeers without any issue , just the hold count increments
    // and still the problem of deadlock is not solved
    // so we are using synchronized blocks whenever required
    private void startElection(){
        synchronized(this){
            if(isLeader()) return;
            leaderId = null;
            transitionToCandidate();
            LOGGER.info("startElection for term {} by {}", currentTerm, nodeId);
            resetElectionTimer(); 
        }
        // below method is synchronized internally and so we dont need to add this in synchronized block
        int votes = requestVoteFromPeers();
        LOGGER.debug("total received votes are {}", votes);
        // because peerAddresses does not include our own address therefore we are adding + 1
        synchronized(this){
            if(isMajority(votes)){
                becomeLeader();
                // i dont think we should send hearbeats here , the job of this function should be to start the election and decide the outcome
                // sendHeartbeats();
            }
            else if(isFollower()) return;
        }

    }

    private synchronized boolean isFollower(){
        return role == NodeRole.FOLLOWER;
    }
    private synchronized boolean isLeader(){
        return role == NodeRole.LEADER;
    }

    private synchronized boolean isCandidate(){
        return role == NodeRole.CANDIDATE;
    }

    private void persistOrDie(){
        boolean ok = raftStateManager.serialize(currentTerm, votedFor, nodeId);
        if(!ok && !stopping){
            LOGGER.error("FATAL: cannot persis raft state (term = {} votedFor = {} nodeId = {} ", currentTerm , votedFor, nodeId);
            System.exit(1);
        }

    }
    private synchronized void transitionToCandidate(){
        role = NodeRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
        persistOrDie();
    }

    // removing synchronized keyword because we dont want to block the main thread
    // we are doing parallel requests to all the peers and holding lock while waiting for response in sendVotesRequestInParallel
    // so we dont want to block the main thread
    private int requestVoteFromPeers(){
        int votes = 1;
        MDC.put("nodeId" , nodeId);
        RequestVoteRequest request = null;
        if(!isCandidate()) return 0;
        synchronized(this){
            LOGGER.debug("term {} node id {} requestVoteFromPeers", currentTerm, nodeId);
            request = buildReqestVoteRequest();
            LOGGER.debug("term {} node id {} vote request {}", currentTerm, nodeId, request);
        }
        List<RequestVoteResponse> responses = sendVotesRequestInParallel(request);

        synchronized(this){
            for(RequestVoteResponse response: responses){
                if(response.getTerm() > currentTerm){
                    stepDownDueToHigherTerm(response.getTerm());
                    return 0;
                }
                else if(response.isVoteGranted())votes++;
            }
        }
        return votes;
    }

    private List<RequestVoteResponse> sendVotesRequestInParallel(RequestVoteRequest request){
        MDC.put("requestId", request.getRequestId());
        // any exception throws by sendRequestVoteToPeer will be caught by exception handler , they are stored in future and unwrapped at future.get()
        // and then caught by catch block so no issues over here.
        if(peerAddresses == null || peerAddresses.size() == 0) return new ArrayList<>();
        List<Future<RpcResult<RequestVoteResponse>>> futures = peerAddresses.stream()
        .map(peer -> rpcExecutor.submit(wrapCallableWithMdc(() -> transport.sendRequestVoteToPeer(request, peer))))
        .collect(Collectors.toList());
        List<RequestVoteResponse> responses = new ArrayList<>();
        LOGGER.debug("term {} node id {} vote request {} to peers {} ", currentTerm, nodeId, request , peerAddresses);
        for(Future<RpcResult<RequestVoteResponse>> future: futures){
            try{
                // 150 is the timeout 
                RpcResult<RequestVoteResponse> result = future.get(300, TimeUnit.MILLISECONDS);
                if(result.isOk())
                    responses.add(result.getResponse());
            }catch(Exception e){
                // not able to get response from other nodes, either peer unreachable or timeout
                LOGGER.error("Exception in sendVotesRequestInParallel {} ", e);
            }
        }
        return responses;
    }

    
    // this method is not synchronized because its called from 
    // requestVoteFromPeers , replicationLoopForPeer,handleRequestVote , handleAppendEntries which are either synchronized or calls from synchronized blocks
    private void stepDownDueToHigherTerm(long term){
        LOGGER.debug("term {} node id {} stepping down due to higher term {} ", currentTerm, nodeId, term);
        currentTerm = term;
        votedFor = null;
        persistOrDie();
        transitionToFollower();
        resetElectionTimer();
        pendingRequests.clear();
        // cancelHeartbeatTimer();
    }
    private RequestVoteRequest buildReqestVoteRequest(){
        String requestId = UUID.randomUUID().toString();
        MDC.put("requestId", requestId);
        LOGGER.debug("term {} node id {} log.lastIndex {} log.lastTerm {} requestId {} ", currentTerm, nodeId, log.lastIndex(), log.lastTerm(), requestId);
        return new RequestVoteRequest(currentTerm, log.lastIndex(), log.lastTerm(), nodeId, requestId);
    }

    private <T> Callable<T> wrapCallableWithMdc(Callable<T> callable){
        Map<String,String> capturedContext = MDC.getCopyOfContextMap();
        return () -> {
            Map<String,String> previousContext = MDC.getCopyOfContextMap();
            try{
                if(capturedContext != null) MDC.setContextMap(capturedContext);
                else MDC.clear();
                if(nodeId != null) MDC.put("nodeId", nodeId);
                return callable.call();
            }finally{
                if(previousContext != null) MDC.setContextMap(previousContext);
                else MDC.clear();
            }
        };
    }

    private Runnable wrapRunnableWithMdc(Runnable runnable){
        Map<String,String> capturedContext = MDC.getCopyOfContextMap();
        return () -> {
            Map<String,String> previousContext = MDC.getCopyOfContextMap();
            try{
                if(capturedContext != null) MDC.setContextMap(capturedContext);
                else MDC.clear();
                if(nodeId != null) MDC.put("nodeId", nodeId);
                runnable.run();
            }finally{
                if(previousContext != null) MDC.setContextMap(previousContext);
                else MDC.clear();
            }
        };
    }

    private synchronized boolean isMajority(int votes){
        return votes >= ((peerAddresses.size() + 1) / 2) + 1;
    }

    private synchronized void transitionToFollower(){
        role = NodeRole.FOLLOWER;
    }
    private  void becomeLeader(){
        // change the role , cancel the election timer, generate fresh peer state and then 
        // send no op leader confirming message and then start sending heartbeats / appendentries,
        // apply those commited index as in when then come as response from followers
        synchronized(this){
            role = NodeRole.LEADER;
            leaderId = nodeId;
            cancelElectionTimer();
            clearPeerState();
        }
            appendNoOpEntryToLog();
        synchronized(this){
            startPerPeerReplicationThread();
            // scheduleHeartbeat();
        }
    }

    
    private void cancelElectionTimer(){
        if(electionTimeoutFuture != null){
            electionTimeoutFuture.cancel(false);
            electionTimeoutFuture = null;
        }
    }
    
    private void clearPeerState(){
        peers.clear();
        for(String peer : peerAddresses){
            peers.put(peer, new PeerState(log.lastIndex() + 1));
        }
    }
    // we first append it to our log and let the peer replication thread handle the replication to other peers, 
    // if there is election tiemout in some other threads and they try to start the election process, then in that this uncommited entry either will be lost / discarded
    // because we wont win the next election as the term for next election will be 1 greater than our current term, so we will not be able to replicate uncommited entry to other peers
    // and we will lose uncommited entry.
    private void  appendNoOpEntryToLog(){
        CompletableFuture<Void> walfuture = null;
        LogEntry noOpEntry = null;
        synchronized(this){ 
            noOpEntry = getNoOpEntry();
            log.append(noOpEntry);
            walfuture = walService.append(new WalRecord(EntryType.ENTRY , noOpEntry, 0));
        }
        // we have to wait for the response from wal node
        // .get() blocks until response is available so we have used timeout
        // waiting outside synchronized block
        try{
            walfuture.get(5 , TimeUnit.SECONDS);
            synchronized(this){
                persistedWalIndex = Math.max(persistedWalIndex, noOpEntry.getIndex());
            }

        }catch(Exception e){
            LOGGER.error("TIMED OUT/Interrupted/Execution Exception \n" +
                "while appending entry to wal , please check stack trace ", e);
                if(!stopping)
                    System.exit(1);
        }
        
    }
    private void startPerPeerReplicationThread(){
        for(String peer : peerAddresses){
            Thread.ofVirtual().start(wrapRunnableWithMdc(() -> replicationLoopForPeer(peer)));
        }
    }

    private void replicationLoopForPeer(String peer){
        // network related failure , sleep time
        // networkFailureSleepTime is the sleep time for network failure
        // we will increase the sleep time by a factor of 2 and max it by 1000 ms
        // for now its 200 ms
        int networkFailureSleepTime = peerRetryBackoffInitialMs;
        MDC.put("nodeId", nodeId);
        LOGGER.debug("term {} node id {} replication loop for peer {} & current node is leader {} ", currentTerm, nodeId, peer, isLeader());
        while(isLeader()){
            try{

                // making a decision to either send appendentries to peer or installsnapshot to peer
                // and for making that decision we will use nextIndex of particualr peer
                // if the nextindex which needs to be replicated/sent to peer is less than our last applied index (last applied 
                // denotes the index of last entry up to which we have taken snapshot of our cache and compacted our wal and truncated our log)
                // that means we have to send install snapsthot to peer
                // boolean nullResponse = false;
                // RpcResult<T> response;
                if(peers.get(peer).getNextIndex() <= log.getLastIncludedIndex()){
                    final InstallSnapshotRequest request = getInstallSnapshotRequest(peer);
                    MDC.put("requestId", request.getRequestId());
                    LOGGER.debug("install snapshot :- term {} node id {} replication to peer {} request {} ", currentTerm, nodeId, peer, request);
                    RpcResult<InstallSnapshotResponse> rpcResult = transport.sendInstallSnapshotToPeer(request , peer);
                    if(rpcResult.isOk()){
                        InstallSnapshotResponse response = rpcResult.getResponse();
                        LOGGER.debug("Received install snapshot response {} ", response);
                        synchronized(this){
                            long term = response.getTerm();
                            if(term > currentTerm){
                                stepDownDueToHigherTerm(term);
                                return;
                            }
                            updatePeerState(response , peer);
                            mayBeAdvanceCommitIndex();
                            networkFailureSleepTime = peerRetryBackoffInitialMs;
                        }
                    }else if(rpcResult.isUnreachable()){
                        LOGGER.debug("Peer {} is unreachable ", peer);
                        LOGGER.debug("Soo, Going to sleepzzzzzzzzz");
                        try{
                            Thread.sleep(heartbeatInterval);
                        }
                        catch(InterruptedException e){
                            LOGGER.debug(" new elements are found but we are interrupted {} ", e);
                        }   
                    }
                    // for timeout and bad response no sleep at all, keep on working :) 
                    
                }
                else{
                    replicationLock.lock();
                    try{
                        if(peers.get(peer).getNextIndex() > log.lastIndex())
                            notNewElements.await(heartbeatInterval , TimeUnit.MILLISECONDS);
                    }catch(InterruptedException e){
                        LOGGER.debug(" new elements are found but we are interrupted {} ", e);
                    }finally{
                        replicationLock.unlock();
                    }
                    final AppendEntriesRequest request = getAppendEntriesRequest(peer);
                    if(!request.getEntries().isEmpty())
                        MDC.put("requestId", request.getEntry(0).getRequestId());
                    LOGGER.debug("term {} node id {} replication to peer {} request {} ", currentTerm, nodeId, peer, request);
                    RpcResult<AppendEntriesResponse> rpcResult = transport.sendAppendEntriesToPeer(request , peer);
                    if(rpcResult.isOk()){
                        AppendEntriesResponse response = rpcResult.getResponse();
                        LOGGER.debug("Received append entries response {} ", response);
                        synchronized(this){
                            long term = response.getTerm();
                            if(term > currentTerm){
                                stepDownDueToHigherTerm(term);
                                return;
                            }
                            updatePeerState(response , peer);
                            mayBeAdvanceCommitIndex();
                            networkFailureSleepTime = peerRetryBackoffInitialMs;
                        }
                    }
                    else if(rpcResult.isUnreachable()){
                        LOGGER.debug("Peer {} is unreachable ", peer);
                        LOGGER.debug("Soo, Going to sleepzzzzzzzzz");
                        try{
                            Thread.sleep(heartbeatInterval);
                        }catch(InterruptedException e){
                            LOGGER.debug(" new elements are found but we are interrupted {} ", e);
                        }
                    }
                }
            }catch(Exception e){
                LOGGER.error("exception in replication loop for peer {} , please check stack trace ", peer, e);
                try {
                    Thread.sleep(1000);
                    break;
                } catch (InterruptedException e1) {
                    LOGGER.error("someone disturbed replication loop for peer {} , please check stack trace ", peer, e1);
                }
            }
            MDC.remove("requestId");
        }
    }

    // so which ever node is declared as leader, will have latest/more accurate value of commited index, in that case its better 
    // to increment it , decreasing it wont make sense because if some new node joined, then it will have commited index as 0 in
    //  that case its not safe/correct to move our leader commited index to 0 because previus nodes which are in this cluster already have agreed on commited index.
    private void mayBeAdvanceCommitIndex(){
        int[] matchIndex = peers.values().stream().mapToInt(PeerState::getMatchIndex).toArray();
        Arrays.sort(matchIndex);
        int majorityMatchIndex = matchIndex[matchIndex.length / 2];
        // if more than half of the nodes have commit index greater than ours and they are in same term then we can advance our commit index
        if(majorityMatchIndex > log.getFirstIndex() && majorityMatchIndex > commitIndex && log.termAt(majorityMatchIndex) == currentTerm){
            commitIndex = Math.min(majorityMatchIndex, persistedWalIndex);
            LOGGER.debug("term {} node id {} advanced commit index {} ", currentTerm, nodeId, commitIndex);
            // to wake up apply thread ,as we have moved our commit index, therefore rest of entries should be applied to cache.
            this.notifyAll();
        }
    }   

    private LogEntry getNoOpEntry(){
        LogEntry noOp = new LogEntry(log.lastIndex() + 1, null , currentTerm ,true, UUID.randomUUID().toString());
        return noOp;
    }

    private synchronized AppendEntriesRequest getAppendEntriesRequest(String peer){
        int prevIndex = peers.get(peer).getNextIndex() - 1;
        long prevTerm = log.termAt(prevIndex);
        List<LogEntry> list = log.getFrom(prevIndex + 1, maxEntriesPerRequest);
        // because leaderId can be null or stale values so its better touse nodeId
        return new AppendEntriesRequest(currentTerm , nodeId , prevIndex, prevTerm, commitIndex , list);
    }

    private synchronized InstallSnapshotRequest getInstallSnapshotRequest(String peer){
        RaftSnapshot raftSnapshot = raftSnapshotManager.deserialize(nodeId);
        String requestId = UUID.randomUUID().toString();
        MDC.put("requestId", requestId);
        LOGGER.debug("current term is {} node Id {} , role {} , lastApplied {} current index {} ", currentTerm, nodeId, getRole(), lastApplied, log.lastIndex());
        LOGGER.debug("lastApplied in raft snapshot {} last term in raft  {} cache state {} ", raftSnapshot.getLastAppliedIndex(), raftSnapshot.getLastAppliedTerm(), raftSnapshot.getCacheState());
        return new InstallSnapshotRequest(currentTerm , nodeId , raftSnapshot.getLastAppliedIndex(), raftSnapshot.getLastAppliedTerm(), raftSnapshot.getCacheState(), requestId);
    }

    // its importatnt to use sychronized keyword in updatedpeerstate because if we dont use 
    // we may think that all we are doing is modifying result or entry of only the follower which is present in response, but we should not forget that 
    // our map (peers) is not concurrent map , that means, if multiple threads tries to read from it even though different entries or keys , it might misbehave
    // so its better to use concurrent map or use synchronized methods to avoid race conditions.
    // for example one thread trying to read some other entry in this updatepeerstate , but some other thread in some other method trying to read that same entry both can have
    // inconsistent info/result available which would be difficult to trace without proper synchronization mechanism
    private synchronized void updatePeerState(Object response, String peer){
        String peerAddress = peer;
        int nextMatchIndex = response instanceof AppendEntriesResponse ? ((AppendEntriesResponse)response).getMatchIndex() : ((InstallSnapshotResponse)response).getAppliedIndex();
        boolean success  = response instanceof AppendEntriesResponse ? ((AppendEntriesResponse)response).isSuccess() : ((InstallSnapshotResponse)response).isSuccess();
        int conflictTermFirstIndex = response instanceof AppendEntriesResponse ? ((AppendEntriesResponse)response).getConflictTermFirstIndex() : -1;
        long conflictTerm = response instanceof AppendEntriesResponse ? ((AppendEntriesResponse)response).getConflictTerm() : -1;
        PeerState state = peers.get(peerAddress);
        LOGGER.debug("term {} and node id {} updating peer {} state {} ", currentTerm, nodeId, peer, state);
        LOGGER.debug("term {} and node id {} match index {} next index {} success {} ", currentTerm, nodeId, state.getMatchIndex(), state.getNextIndex(), success);
        LOGGER.debug("term {} and node id {} response {} ", currentTerm, nodeId, response);
        if(nextMatchIndex > state.getMatchIndex()) state.setMatchIndex(nextMatchIndex);
        if(success) state.setNextIndex(state.getMatchIndex() + 1);
        else if(state.getNextIndex() > 1 ) {
            if(response instanceof AppendEntriesResponse && conflictTermFirstIndex != -1 && conflictTerm != -1){
                if(log.termAt(conflictTermFirstIndex) == conflictTerm && conflictTermFirstIndex <= log.lastIndex() && conflictTermFirstIndex >= log.getLastIncludedIndex()){
                    state.setNextIndex(conflictTermFirstIndex);
                }else{
                    state.setNextIndex(state.getMatchIndex() + 1);
                }
            }else{
                state.setNextIndex(state.getMatchIndex() + 1);
            }
        }

    }
    // this method shouldn't be synchronized because we are using virtual threads to replicate the log and we dont want to block the main thread
    // and each virtual thread is handling replication to single node, so there is no need of synchronization among them.
    
    // private synchronized void sendHeartbeats(){
    // }
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest requestVoteRequest){
        // some node is asking for vote
        // Seeing a higher term means you must update your term. It does not mean you must grant the vote.
        // lets say a node which is asking us for vote has term x > our current term, what does this mean 
        //  it means it has gone through more election timeouts and has seens more terms but that does not imply that it has more updated logs
        // becaues it can happen that in cluster of 5 nodes , due to partition a group of nodes can keep on particicpating in election becaues they are not able to reach
        // to majority thus just having more/larger term does not gurantee that the node is supposed to be leader, but it does mean that the current node was not part of all those
        // terms and there fore its better to step down and let all nodes agree on this current term and then continue the leader election process.
        MDC.put("nodeId", nodeId);
        LOGGER.debug("term {} node id {} handling request vote {} ", currentTerm, nodeId, requestVoteRequest);
        RequestVoteResponse response = buildRequestVoteResponse(false);
        // we have gone through more election terms so we are more updated than the node who is asking for vote thus its best not to grant it vote
        if(requestVoteRequest.getTerm() < currentTerm){
            return response;
        }
        // now the node who is asking for votes has seen more term than us, it means it can be updated but we have to check that
        if(requestVoteRequest.getTerm() > currentTerm){
            stepDownDueToHigherTerm(requestVoteRequest.getTerm());
            response.setTerm(requestVoteRequest.getTerm());
        }
        // if candidate node has term same as ours or greater than ours but lets verify if its atleast updated as us. because we let any leader win with stale entries, because that would mean
        // it would send us empty entries to append ,which is wrong.
        if (log.isUpToDate(requestVoteRequest.getLastLogIndex(), requestVoteRequest.getLastLogTerm())){
            if(votedFor == null || votedFor.equals(requestVoteRequest.getCandidateId())){
                votedFor = requestVoteRequest.getCandidateId();
                resetElectionTimer();
                LOGGER.debug("going to call raftstatemanager for serializations {} {} {} ", currentTerm, votedFor, nodeId);
                boolean grantVote = raftStateManager.serialize(currentTerm, votedFor, nodeId);
                if(grantVote)
                    response.setVoteGranted(true);
                else
                    response.setVoteGranted(false);
                LOGGER.debug("term {} node id {} vote granted for term {} ", currentTerm, nodeId, requestVoteRequest.getTerm());
                return response;
            }
        }
        LOGGER.debug("term {} node id {} vote denied <<->> requesstvotelastlogindex {} requestvotelastlogterm {} ", currentTerm, nodeId, requestVoteRequest.getTerm(), requestVoteRequest.getLastLogIndex(), requestVoteRequest.getLastLogTerm());
        LOGGER.debug("response from node is {} ", response);
        return response;
    }

    public RequestVoteResponse buildRequestVoteResponse(boolean voteGranted){
        RequestVoteResponse response = new RequestVoteResponse();
        response.setFollowerId(nodeId);
        response.setTerm(currentTerm);
        response.setVoteGranted(voteGranted);
        return response;
    }
    /*
    handle append entries are those entries which are sent by leaders
    1. we must check the leader which sent these append entries are same as the leader which we know should be leader
    like is some one masqueading or altering our payload. 
    2. the entries which we have received are as same term as the current term
    3. once we have received this message that means we have received heartbeat that means we have to reest our
    election timer , because leader is active .
    4. before appending we have to ensure that we have prevLogTerm and prevLogIndex same as what is sent in appendentries
        if these does not match then we nack them else we ack them and update our entries.
    5. if it passes we append those entries and update our commit index 
    6. follower will update its commit index as leader commit
    7. followerId  will be same irrespective of fail/success. term can increase or decrease depending on success/failure
    if current node has no entry in suggested position or log does not match in that case success = false else it will be true
    term will be requests term because the node which will look at these responses should know for which term it received
    the response matchIndex = last log index we have successfully appended in our log
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request){
        MDC.put("nodeId", nodeId);
        if(!request.getEntries().isEmpty())
            MDC.put("requestId", request.getEntry(0).getRequestId());
        try{

            LOGGER.debug("term {} node id {} handling append entries request {} ", currentTerm, nodeId, request);
            CompletableFuture<Void> walfuture = null;
            AppendEntriesResponse response = builAppendEntriesResponse();
            synchronized(this){
                if(request.getTerm() < currentTerm){
                    return response;
                }
                // we cannot be leader if node who is asking for appending entries has more term than us
                if(request.getTerm() > currentTerm){
                    stepDownDueToHigherTerm(request.getTerm());
                }
        
                // if(role == NodeRole.LEADER)cancelHeartbeatTimer();
                transitionToFollower();
        
                leaderId = request.getLeaderId();
                // hearbeats interval/append entries interval << election timeout i.e. before election timeout we will send hearbeats/appendentries
                resetElectionTimer();
                // whether the node which is asking for appending entry has updated log 
                if(!log.hasMatchAt(request.getPrevLogIndex() , request.getPrevLogTerm())){
                    LOGGER.debug("i guess we are returnig from here");
                    if(request.getPrevLogIndex() > log.lastIndex()){
                        // no conflict term only conflict index 
                        response.setConflictTermFirstIndex(log.lastIndex() + 1);
                    }else{
                        long conflictTerm = log.termAt(request.getPrevLogIndex());
                        int conflictTermFirstIndex = log.getFirstIndex(conflictTerm, request.getPrevLogIndex());
                        response.setConflictTermFirstIndex(conflictTermFirstIndex);
                        response.setConflictTerm(conflictTerm);
                    }
                    return response;
                }
                // if the node which is asking us to append entry has updated log then we can safely append it in our log
                if(log.lastIndex() >= request.getPrevLogIndex() + 1){
                    walfuture = walService.append(new WalRecord(EntryType.TRUNCATE , null, request.getPrevLogIndex() + 1));
                }
                // we have to wait for the response from wal node
                // .get() blocks until response is available so we have used timeout
                
                // we have to append all the entries in the request to our wal log and then wait for it complete
                for(LogEntry entry : request.getEntries()){
                    walfuture = walService.append(new WalRecord(EntryType.ENTRY , entry, 0));
                }
                // waiting outside synchronized block
                /*
                breaking change of sept22 2026
                the reason we have segregated wal and log append is because wal is a single threaded
                so there is chance that while we are truncating our log , and releasing lock on raftlog
                apply loop runs and tries to read entries which is < commit index and we might not have that
                entry in our log and that will raise array index out of bounds exception while reading from log
                and this exception will failstop our system
                */
            }
            if(walfuture != null){
                try{
                    walfuture.get(5 , TimeUnit.SECONDS);
                }catch(Exception e){
                    LOGGER.error("TIMED OUT/Interrupted/Execution Exception \n" +
                        "while appending entry to wal , please check stack trace ", e);
                    MDC.remove("requestId");
                    return response;
                }
            }
            synchronized(this){
                if(log.lastIndex() >= request.getPrevLogIndex() + 1){
                    log.truncateFrom(request.getPrevLogIndex() + 1);
                }
                
                for(LogEntry entry : request.getEntries()){
                    log.append(entry);
                }
            
                // as we have updated our log , we need to move/change our commit index as well
                if(request.getLeaderCommit() > commitIndex){
                    commitIndex = Math.min(request.getLeaderCommit() , log.lastIndex());
                }
                // because we are sure our wal succeeded and persisted wal idnex denotes which/how many entries are successfully persisted in wal
                // and in this case the las log index denotes the same
                persistedWalIndex = log.lastIndex();
        
                // using apply executor to apply entrires in cache , this is single threaded executor because we dont want multiple
                // threads trying to change cache state as order of opeartions are importnat.
                // we just notify waiting applyexecutor thread and that's all rest of things will be taken care by woken up thread.
                this.notifyAll();
                // match index tell the leader to sent next index  = log.lastlogindex + 1
                response.setMatchIndex(log.lastIndex());
                response.setSuccess(true);
                response.setTerm(currentTerm);
                LOGGER.debug("term {} node id {} enteries appended in log response {} ", currentTerm, nodeId, response);
            }   
            MDC.remove("requestId");
            return response;
        }finally{
            MDC.remove("requestId");
        }

    }

    public AppendEntriesResponse builAppendEntriesResponse(){
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setSuccess(false);
        response.setTerm(currentTerm);
        response.setFollowerId(nodeId);
        response.setMatchIndex(commitIndex);
        response.setConflictTermFirstIndex(-1);
        response.setConflictTerm(-1);
        return response;

    }

    // once we receive response from leader we have to 
    // 1. if the term is current term which is ongoing
    //    a. if term is higher then change our current term to what leader has sent and accept the snapshot
    //    b. if term is lower, then ask leader to step down and wait for new election
    // 2. once we have successfully parsed the snapshot, first of all take the snapshot of our current state , clear wal and log , change our 
    //    current term , last applied index and commit index to what leader has sent and then restore the state of cache from snapshot  
    //    the reason being, lets say we restored our snapstho and then tried to take snapstho and if it failes, it might happen that we have told our leader
    //    that we have applied some index and our leader has committed to our client that entry is replicated and turns out our current node restart and now we are 
    //    back to square one. 
    public InstallSnapshotResponse handleInstallSnapshot(InstallSnapshotRequest request){
        MDC.put("nodeId", nodeId);
        MDC.put("requestId", request.getRequestId());
        LOGGER.debug("Install snapshot request is {} ", request);
        InstallSnapshotResponse response = buildInstallSnapshotResponse();
        synchronized(this){
            // now the node who is asking for votes has seen more term than us, it means it can be updated but we have to check that
            if(request.getTerm() < currentTerm){
                LOGGER.debug("term {} node id {} install snapshot request is less than current term {} ", request.getTerm(), nodeId, currentTerm);
                return response;
            }
            if(request.getTerm() > currentTerm){
                stepDownDueToHigherTerm(request.getTerm());
                response.setTerm(request.getTerm());
            }

            transitionToFollower();
            leaderId = request.getLeaderId();
            resetElectionTimer();
            
            if(request.getLastIncludedIndex() <= lastApplied){
                LOGGER.debug("NACK Install snapshot request as term {} is less than current term {} or lastApplied {} is less than lastIncludedIndex {} ", request.getTerm(), currentTerm, lastApplied, request.getLastIncludedIndex());
                response.setAppliedIndex(lastApplied);
                return response;
            }
            final Map<String, CacheItem> cacheState = request.getCacheState();
            Map<String , String> saved = MDC.getCopyOfContextMap();
            // Thread.ofVirtual().start(() -> {
                if(saved != null) MDC.setContextMap(saved);
                LOGGER.debug("lastApplied is {} and request.getLastIncludedIndex() is {} lastIncludedTerm is {} nodeId is {} ", lastApplied, request.getLastIncludedIndex(), request.getLastIncludedTerm(), nodeId);
                LOGGER.debug("cache state is {} ", cacheState);
                boolean snapshotStatus = raftSnapshotManager.serialize(cacheState, request.getLastIncludedIndex(), request.getLastIncludedTerm(), nodeId);
                LOGGER.debug("before entering snapshot status is {} ", snapshotStatus);
                synchronized(this){
                    LOGGER.debug("snapshot status is {} ", snapshotStatus);
                    if(snapshotStatus == true){
                        log = new RaftLog(request.getLastIncludedIndex(), request.getLastIncludedTerm());
                        log.setLastIncludedIndex(request.getLastIncludedIndex());
                        log.setLastIncludedTerm(request.getLastIncludedTerm());
                        cache.restoreState(request.getCacheState());
                        lastApplied = request.getLastIncludedIndex();
                        commitIndex = request.getLastIncludedIndex();
                        // last applied means how many we have applied to our cache , from our logs , but this is cache restoration , 
                        // we havent  applied from our logs, so increasing last applied would be wrong, 
                        LOGGER.debug("lastApplied is {} and commitIndex is {} ", lastApplied, commitIndex);
                        // below applied index means they are successfully applied to cache, it has nothing to do with our raft logs size.
                        response.setAppliedIndex(request.getLastIncludedIndex());
                        response.setTerm(currentTerm);
                        response.setSuccess(true);
                        persistedWalIndex = log.lastIndex();


                    }
                }
            // });
        }
        LOGGER.debug("sending install snapsthot response {} ", response);
        

        return response;
    }

    public InstallSnapshotResponse buildInstallSnapshotResponse(){
        InstallSnapshotResponse response = new InstallSnapshotResponse();
        response.setTerm(currentTerm);
        response.setFollowerId(nodeId);
        response.setAppliedIndex(lastApplied);
        response.setSuccess(false);
        return response;

    }
    // this is called by server to write commands to cache
    // it basically returns a completeable future <string> beaus string is sreturn type of our commands and 
    // completable future because its async in nature, we want to ensure that our entry is replicated to majority of nodes, 
    // leaders commit the entry , and apply loop executes the commadn in our cache, once all these 3 are done , we complete the future.
    // we append an entry at log at index lets say n , and store completable future at index n in map
    // and return future immediately , after that per thread replication starts, (replicationLoopForPeer)
    // thta loops replicates our entry in all the peers and move the comimt index, while updating our commit index
    // we call maybeadvancecommit() which notifies all waiting thread, that would wake up applycommitedentries threads
    // and once we able to apply that entry to our cache we complete our future and return stored as result
    public CompletableFuture<String> propose(String command){

        CompletableFuture<String> future = new CompletableFuture<>();
        CompletableFuture<Void> walfuture = null;
        LogEntry logEntry = null;
        String requestId = UUID.randomUUID().toString();
        MDC.put("nodeId", nodeId);
        try{
            synchronized(this){
                MDC.put("requestId", requestId);
                if(!isLeader()){
                    String message = leaderId == null ? "Election in progress " : "Not Leader: "+leaderId;
                    future.completeExceptionally(new IllegalStateException(message));
                    return future;
                }
                int index = log.lastIndex() + 1;
                /*
                the reason we are doing first append in in-memory log and then appending to wal is that
                even if lets say entry is inserted in in-memory log and then we crash and restart the node
                the entry wont be restored and  wal wont have it. so its not an issue, 
                but lets say our wal had it and then we crashed and restored
                then in-memory log will eventually have it.
                one case to note is that , by the the time, we insert the entry in in-memory log and we are waiting for fsync on wal 
                to complete, leader can send its entry to follower and commit it, and now if our fsync fails , in that case we will need
                persistedIndex as one variable which keep track of which index are actually persisted in wal and in-memory log.
                for now we are not considering that case.
                 */
                logEntry = new LogEntry(index, command, currentTerm, false, requestId);
                log.append(logEntry);
                pendingRequests.put(index, future);
                walfuture = walService.append(new WalRecord(EntryType.ENTRY , logEntry, 0));
                LOGGER.debug("client proposed entry to leader requestId is {} stored at index {} and lastApplied is {} ", requestId, index, lastApplied);
            }

            
            try{
                walfuture.get(5 , TimeUnit.SECONDS);
                synchronized(this){
                    // we need max on this because we dont want to go in backwards direction , if wal for entry 4 succeeded then wal for 
                    // 1,2,3 will succeed, just because response of wal future 1 came back later does not imply persisted wal index is 1
                    persistedWalIndex = Math.max(persistedWalIndex, logEntry.getIndex());
                }
            }catch(TimeoutException | InterruptedException e){
                // this means we will have entry in wal and we have entry in raftlog, but wal is not done yet
                // we cant delete raft log entry because was entry will succeed 
                // but this means get on same key k might succeed, yeah raft never gurantees anything about failed writes
                // it only says if that entry is stored then no way we are going to loose that entry.
                // future.completeExceptionally(e);
                // return future;
                // dont return here, let replicatino loop know we have new entry it can start replicating
                LOGGER.debug("time out exception in wal future ");
            }catch(Exception e){
                synchronized(this){
                    int index = logEntry.getIndex();
                    log.truncateFrom(persistedWalIndex + 1);
                    pendingRequests.remove(index);
                    LOGGER.error("Couldn't append entry to log :( , please check stack trace ", e);
                    future.completeExceptionally(e);
                    return future;
                }
            }

            replicationLock.lock();
            try{
                notNewElements.signalAll();
            }finally{
                replicationLock.unlock();
            }

        }finally{
            MDC.remove("requestId");
            MDC.remove("nodeId");
        }

        return future;
    }

    /**
     * backgroud loop which applied commited entires to our cache
     * a dead apply loop means this node can never make progress again, so it fail-stops instead.
     */
    private void applyCommitedEntries(){
        try{
            applyCommitedEntriesLoop();
        }catch(Throwable t){
            if(stopping) return;   // stop() interrupts this thread; that is not a failure
            FailStop.halt(LOGGER, "apply loop died on " + nodeId + " at lastApplied=" + lastApplied, t);
        }
    }

    private void applyCommitedEntriesLoop(){
        MDC.put("nodeId", nodeId);
        while(!Thread.currentThread().isInterrupted()){
            
            synchronized(this){
                while(lastApplied >= commitIndex){
                    try{
                        this.wait();
                    }catch(InterruptedException e) {
                        return;
                    }
                }
                LOGGER.debug("before applying commits lastApplied is {} and commitIndex is {} ", lastApplied, commitIndex);
                
                if(lastApplied >= snapShotThreshold && !inProgress){
                    LOGGER.debug("Snapshot is in progress");
                    // as our above condition says that we have > snapShotThreshold entris 
                    // so we take that state and make sure even in background values of lastapplied , current term or cache state changes
                    // it shouldn't affect our snapshot.
                    inProgress = true;
                    final int snapShotApplied = lastApplied;
                    final long snapShotTerm = log.termAt(snapShotApplied);
                    final Map<String, CacheItem> cacheState = cache.getState();
                    LOGGER.debug("lastApplied is {} and snapShotApplied is {} ", lastApplied, snapShotApplied);
                    LOGGER.debug("cache state is {} ", cacheState);
                    Map<String , String> saved = MDC.getCopyOfContextMap();
                    Thread.ofVirtual().start(() -> {
                        if(saved != null) MDC.setContextMap(saved);
                        boolean snapshotStatus = raftSnapshotManager.serialize(cacheState, snapShotApplied, snapShotTerm , nodeId);
                        // need to lock on this object because we are using virtual threads
                        // without locking we might change the state while some thread is reading it
                        try{
                            synchronized(this){
                            if(snapshotStatus == true){
                                boolean compactStatus = log.compactTill(snapShotApplied);
                                if(compactStatus == false){
                                    LOGGER.error("error while compacting log entries");
                                    return;
                                }
                                log.setLastIncludedIndex(snapShotApplied);
                                log.setLastIncludedTerm(snapShotTerm);
                                // it may seem that doing this wal compaction async is wrong, but we are doing it because we know we have taken cache snapstho and logs are turncated, so even
                                // if wal compaction fails we have snapsthots from where we can restore our cache and later apply wal entries after last included index
                                LOGGER.debug("added an entry to do the compaction in wal");
                                walService.append(new WalRecord(EntryType.COMPACT, null, snapShotApplied));
                                // LOGGER.debug("print file channel after compaction");
                                // walService.printFileChannel();
                                // after lastapplied > snapshot threshold, each lastapplied + x will trigger snapshot
                                // to avoid that we keep on increasing snapsthot threshold. that way 
                                // lastapplied +x wont trigger snapshot until its greater than > lastapplied + 1000
                                // basically x > 1000
                                snapShotThreshold = snapShotApplied + snapShotLimit;
                                }
                            }
                        }finally{
                            MDC.clear();
                        }
                        inProgress = false;
                    });
                    
                    
                }
                // our clients/leaders entries are applied from index 1 not 0 therefore we are incrementing lastApplied by 1
                // also our inserts in log are at log.lastIndex() + 1 not log.lastIndex() so this helps us to get the correct index
                lastApplied++;
                if(lastApplied > log.lastIndex()) throw new RuntimeException("commit index is greater than last applied index");
                LogEntry entry = log.get(lastApplied);
                MDC.put("requestId",  entry.getRequestId());
                Response response = null;
                LOGGER.debug("term {} node id {} applying entry {} at index {} ", currentTerm, nodeId, entry, lastApplied);
                if(!entry.isNoOp()) {
                    // deserialize sits inside the try too. Every node parses the same bytes and fails
                    // the same way, so each skips the same entry and they stay identical. Outside the
                    // try, one entry it could not parse killed this loop on every node at once.
                    try{
                        Command command = Command.deserialize(entry.getCommand());
                        response = CommandProcessor.process(command, cache);
                    } catch (Exception e) {
                        LOGGER.error("failed to apply command at index {} : {} - {}",
                                    lastApplied, e.getClass().getSimpleName(), e.getMessage());
                        LOGGER.debug("stack trace for failed apply at index {}", lastApplied, e);
                    }

            
                }
                CompletableFuture<String> future = pendingRequests.remove(lastApplied);
                if(future != null && response != null) {
                    future.complete(response.toProtocolString());
                }
                else if(future != null) future.complete("SERVER_ERROR\r\n");
                // when we have applied an entry in our cache then only we will increment lastApplied
                LOGGER.debug("after applying commits lastApplied is {} and commitIndex is {} ", lastApplied, commitIndex);
                MDC.remove("requestId");
                LOGGER.debug("applied term {} node id {} applied entry {} at index {} is done", currentTerm, nodeId, entry, lastApplied);
            }
        }
        MDC.remove("nodeId");
    }
    
    private void resetElectionTimer(){
        // the job of this method is to cancel election timer
        // and restart after some random time
        if(electionTimeoutFuture != null){
            electionTimeoutFuture.cancel(false);
        }
        long randomTime = ThreadLocalRandom.current().nextLong(minElectionTimeout, maxElectionTimeout);
        Runnable runnable = wrapRunnableWithMdc(() -> startElection());
        electionTimeoutFuture = scheduler.schedule(
            // start election
            runnable
        , randomTime, TimeUnit.MILLISECONDS);
    }

    public synchronized LinkedHashMap<String , String> getStats(){
        /*
        STAT curr_items 42
        STAT raft_node_id node1
        STAT raft_role LEADER
        STAT raft_term 4
        STAT raft_commit_index 87
        STAT raft_last_applied 87
        STAT raft_leader_id node1
        STAT raft_last_included_index 80
        STAT raft_log_size 8
        END
         */
        LinkedHashMap<String , String> stats = new LinkedHashMap<>();
        stats.put("curr_items" , getCache().size() + "");
        stats.put("raft_node_id" , nodeId+"");
        stats.put("raft_role" , getRole().toString());
        stats.put("raft_term" , getTerm() + "");
        stats.put("raft_commit_index" , commitIndex + "");
        stats.put("raft_last_applied" , lastApplied + "");
        if(leaderId != null) stats.put("raft_leader_id" , leaderId + "");
        stats.put("raft_last_included_index" , getLog().getLastIncludedIndex() + "");
        // avoiding sentinel entry in log
        stats.put("raft_log_size" , (getLog().size() - 1) + "");
        return stats;
    }

    // some helper methods
    public CacheItem get(String key){
        return cache.get(key);
    }

    public NodeRole getRole(){
        return role;
    }

    public String getNodeId(){
        return nodeId;
    }

    public void stop(){
        stopping = true;
        synchronized(this) {
            transitionToFollower();   // holds lock, correctly writes role
            cancelElectionTimer();    // cancels pending timer before shutdown
        }

        rpcExecutor.shutdownNow();
        scheduler.shutdownNow();
        walService.shutdown();
        applyExecutor.shutdownNow();
    }

    public synchronized long getTerm(){
        return currentTerm;
    }   

    public List<String> getPeerAddressList(){
        return this.peerAddresses;
    }

    // pacakge private
    RaftLog getLog(){
        return log;
    }

    Cache getCache(){
        return cache;
    }

    String votedFor(){
        return votedFor;
    }

    public int raftLogSize(){
        return log.size();
    }

    public boolean serviceShuttingDown(){
        return stopping;
    }
}

