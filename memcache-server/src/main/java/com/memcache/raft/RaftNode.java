package com.memcache.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.Arrays;
import com.memcache.cache.CacheItem;
import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.handler.CommandProcessor;
import com.memcache.response.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
public class RaftNode{
    
    // persistent fields
    private int      currentTerm;
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
    // private       ScheduledFuture<?>       heartbeatTimeoutFuture;
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

    private Logger LOGGER = LoggerFactory.getLogger(RaftNode.class.getName());

    public RaftNode(List<String> peerAddresses, String nodeId, Cache cache, RaftTransport transport) {
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
        //  a timer to wait for heartbeat from leader
        resetElectionTimer();
        this.applyExecutor.submit(this::applyCommitedEntries);
    }
    private synchronized void startElection(){
        LOGGER.info("startElection for term {} by {}", currentTerm, nodeId);
        if(isLeader()) return;
        transitionToCandidate();
        resetElectionTimer(); 
        int votes = requestVoteFromPeers();
        // because peerAddresses does not include our own address therefore we are adding + 1
        if(isMajority(votes)){
            becomeLeader();
            // i dont think we should send hearbeats here , the job of this function should be to start the election and decide the outcome
            // sendHeartbeats();
        }
        else if(isFollower()) return;

    }

    private synchronized boolean isFollower(){
        return role == NodeRole.FOLLOWER;
    }
    private synchronized boolean isLeader(){
        return role == NodeRole.LEADER;
    }

    private synchronized void transitionToCandidate(){
        role = NodeRole.CANDIDATE;
        currentTerm++;
        votedFor = nodeId;
    }

    private synchronized int requestVoteFromPeers(){
        int votes = 1;
        LOGGER.info("requestVoteFromPeers for term {} by {}", currentTerm, nodeId);
        RequestVoteRequest request = buildReqestVoteRequest();
        LOGGER.info("term {} node id {} vote request {}", currentTerm, nodeId, request);
        List<RequestVoteResponse> responses = sendVotesRequestInParallel(request);
        for(RequestVoteResponse response: responses){
            if(response.getTerm() > currentTerm){
                stepDownDueToHigherTerm(response.getTerm());
                return 0;
            }
            else if(response.isVoteGranted())votes++;
        }
        return votes;
    }

    private List<RequestVoteResponse> sendVotesRequestInParallel(RequestVoteRequest request){
        // any exception throws by sendRequestVoteToPeer will be caught by exception handler , they are stored in future and unwrapped at future.get()
        // and then caught by catch block so no issues over here.
        List<Future<RequestVoteResponse>> futures = peerAddresses.stream()
        .map(peer -> rpcExecutor.submit(() -> {
            return transport.sendRequestVoteToPeer(request, peer);
            // return sendRequestVoteToPeer(peer, request);
        })).collect(Collectors.toList());
        List<RequestVoteResponse> responses = new ArrayList<>();
        LOGGER.info("term {} node id {} vote request {} to peers {} ", currentTerm, nodeId, request , peerAddresses);
        for(Future<RequestVoteResponse> future: futures){
            try{
                // 150 is the timeout 
                RequestVoteResponse response = future.get(150, TimeUnit.MILLISECONDS);
                if(response != null)
                    responses.add(response);
            }catch(Exception e){
                // not able to get response from other nodes, either peer unreachable or timeout
                LOGGER.error("Exception in sendVotesRequestInParallel {} ", e.getMessage());
            }
        }
        return responses;
    }

    

    private void stepDownDueToHigherTerm(int term){
        LOGGER.info("term {} node id {} stepping down due to higher term {} ", currentTerm, nodeId, term);
        currentTerm = term;
        votedFor = null;
        transitionToFollower();
        resetElectionTimer();
        // cancelHeartbeatTimer();
    }
    private RequestVoteRequest buildReqestVoteRequest(){
        LOGGER.info("term {} node id {} log.lastIndex {} log.lastTerm {} ", currentTerm, nodeId, log.lastIndex(), log.lastTerm());
        return new RequestVoteRequest(currentTerm, log.lastIndex(), log.lastTerm(), nodeId);
    }
    private synchronized boolean isMajority(int votes){
        return votes >= ((peerAddresses.size() + 1) / 2) + 1;
    }

    private synchronized void transitionToFollower(){
        role = NodeRole.FOLLOWER;
    }
    private synchronized void becomeLeader(){
        // change the role , cancel the election timer, generate fresh peer state and then 
        // send no op leader confirming message and then start sending heartbeats / appendentries,
        // apply those commited index as in when then come as response from followers
        role = NodeRole.LEADER;
        leaderId = nodeId;
        cancelElectionTimer();
        clearPeerState();
        appendNoOpEntryToLog();
        startPerPeerReplicationThread();
        // scheduleHeartbeat();
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
        log.append(getNoOpEntry());
        
    }
    private void startPerPeerReplicationThread(){
        for(String peer : peerAddresses){
            Thread.ofVirtual().start(() -> { replicationLoopForPeer(peer);});
        }
    }

    private void replicationLoopForPeer(String peer){
        // network related failure , sleep time
        // networkFailureSleepTime is the sleep time for network failure
        // we will increase the sleep time by a factor of 2 and max it by 1000 ms
        // for now its 200 ms
        int networkFailureSleepTime = 200;
        LOGGER.info("term {} node id {} replication loop for peer {} & current node is leader {} ", currentTerm, nodeId, peer, isLeader());
        while(isLeader()){
            
            final AppendEntriesRequest request = getAppendEntriesRequest(peer);
            LOGGER.info("term {} node id {} replication to peer {} request {} ", currentTerm, nodeId, peer, request);
            AppendEntriesResponse response = transport.sendAppendEntriesToPeer(request , peer);
            // AppendEntriesResponse response = sendAppendEntriesToPeerInParallel(request , peer);
            LOGGER.info("term {} node id {} response from peer {} ", currentTerm, nodeId, peer);
            if(response == null){
                // if our response is null , try after some time , this is to avoid infinite loop
                // in case of some network issue
                try{
                    Thread.sleep(networkFailureSleepTime);
                    // increase the network failure sleep time by a factor of 2 and max it by 1000 ms
                    networkFailureSleepTime = Math.min(networkFailureSleepTime * 2, 1000);
                }catch(InterruptedException e){
                    LOGGER.error("print stacktrace", e);

                }
                continue;
            } 
            // we are adding sleep because , if we dont have anything else to replicate, withoout sleep we will continously overwhelm the system
            // which will flood the system , also our while loop is tight spin , meaning it will keep on sending heartbeats/appendentries continously
            if(response.isSuccess() && request.getEntries().isEmpty()){
                try{
                    // sleep for 100 ms
                    Thread.sleep(100);
                }catch(InterruptedException e){
                                LOGGER.error("print stacktrace", e);
;
                }
            }
            
            synchronized(this){
                if(response.getTerm() > currentTerm){
                    stepDownDueToHigherTerm(response.getTerm());
                    return;
                }
                updatePeerState(response , peer);
                mayBeAdvanceCommitIndex();
                networkFailureSleepTime = 200;
            }
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
        if(majorityMatchIndex > commitIndex && log.termAt(majorityMatchIndex) == currentTerm){
            commitIndex = majorityMatchIndex;
            LOGGER.info("term {} node id {} may advance commit index {} ", currentTerm, nodeId, commitIndex);
            // to wake up apply thread ,as we have moved our commit index, therefore rest of entries should be applied to cache.
            this.notifyAll();
        }
    }   

    private LogEntry getNoOpEntry(){
        LogEntry noOp = new LogEntry(log.lastIndex() + 1, null , currentTerm ,true);
        return noOp;
    }

    // private void scheduleHeartbeat(){
    //     // we need to send heartbeats to all peers
    //     // send heratbeats every 50 ms
    //     heartbeatTimeoutFuture = scheduler.scheduleAtFixedRate(() -> {
    //         // start election
    //         sendHeartbeats();
    //     }, 0,50, TimeUnit.MILLISECONDS);
    // }

    // private void cancelHeartbeatTimer(){
    //     if(heartbeatTimeoutFuture != null){
    //         heartbeatTimeoutFuture.cancel(false);
    //         heartbeatTimeoutFuture = null;
    //     }
    // }

    private synchronized AppendEntriesRequest getAppendEntriesRequest(String peer){
        int prevIndex = peers.get(peer).getNextIndex() - 1;
        int prevTerm = log.termAt(prevIndex);
        List<LogEntry> list = log.getFrom(prevIndex + 1);
        // because leaderId can be null or stale values so its better touse nodeId
        return new AppendEntriesRequest(currentTerm , nodeId , prevIndex, prevTerm, commitIndex , list);
    }
    // its importatnt to use sychronized keyword in updatedpeerstate because if we dont use 
    // we may think that all we are doing is modifying result or entry of only the follower which is present in response, but we should not forget that 
    // our map (peers) is not concurrent map , that means, if multiple threads tries to read from it even though different entries or keys , it might misbehave
    // so its better to use concurrent map or use synchronized methods to avoid race conditions.
    // for example one thread trying to read some other entry in this updatepeerstate , but some other thread in some other method trying to read that same entry both can have
    // inconsistent info/result available which would be difficult to trace without proper synchronization mechanism
    private synchronized void updatePeerState(AppendEntriesResponse response, String peer){
        String followerId = response.getFollowerId();
        String peerAddress = peer;
        int nextMatchIndex = response.getMatchIndex();
        boolean success  = response.isSuccess();
        PeerState state = peers.get(peerAddress);
        LOGGER.info("term {} and node id {} updating peer {} state {} ", currentTerm, nodeId, peer, state);
        LOGGER.info("term {} and node id {} match index {} next index {} success {} ", currentTerm, nodeId, state.getMatchIndex(), state.getNextIndex(), success);
        LOGGER.info("term {} and node id {} response {} ", currentTerm, nodeId, response);
        state.setMatchIndex(nextMatchIndex);
        if(success) state.setNextIndex(state.getMatchIndex() + 1);
        else if(state.getNextIndex() > 1 ) state.setNextIndex(state.getNextIndex() - 1);

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
        LOGGER.info("term {} node id {} handling request vote {} ", currentTerm, nodeId, requestVoteRequest);
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
                response.setVoteGranted(true);
                LOGGER.info("term {} node id {} vote granted for term {} ", currentTerm, nodeId, requestVoteRequest.getTerm());
                return response;
            }
        }
        LOGGER.info("term {} node id {} vote denied <<->> requesstvotelastlogindex {} requestvotelastlogterm {} ", currentTerm, nodeId, requestVoteRequest.getTerm(), requestVoteRequest.getLastLogIndex(), requestVoteRequest.getLastLogTerm());
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
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request){
        LOGGER.info("term {} node id {} handling append entries request {} ", currentTerm, nodeId, request);
        AppendEntriesResponse  response = builAppendEntriesResponse();
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
            response.setSuccess(false);
            return response;
        }
        // if the node which is asking us to append entry has updated log then we can safely append it in our log
        log.truncateFrom(request.getPrevLogIndex() + 1);
        for(LogEntry entry : request.getEntries())log.append(entry);

        // as we have updated our log , we need to move/change our commit index as well
        if(request.getLeaderCommit() > commitIndex)
            commitIndex = Math.min(request.getLeaderCommit() , log.lastIndex());

        // using apply executor to apply entrires in cache , this is single threaded executor because we dont want multiple
        // threads trying to change cache state as order of opeartions are importnat.
        // we just notify waiting applyexecutor thread and that's all rest of things will be taken care by woken up thread.
        this.notifyAll();
        // match index tell the leader to sent next index  = log.lastlogindex + 1
        response.setMatchIndex(log.lastIndex());
        response.setSuccess(true);
        response.setTerm(currentTerm);
        LOGGER.info("term {} node id {} append entries response {} ", currentTerm, nodeId, response);
        return response;

    }

    public AppendEntriesResponse builAppendEntriesResponse(){
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setSuccess(false);
        response.setTerm(currentTerm);
        response.setFollowerId(nodeId);
        response.setMatchIndex(commitIndex);
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
        synchronized(this){
            if(!isLeader()){
                future.completeExceptionally(new IllegalStateException("Not leader: " + leaderId));
                return future;
            }
            int index = log.lastIndex() + 1;
            log.append(new LogEntry(index, command, currentTerm, false));
            pendingRequests.put(index, future);
        }

        return future;
    }

    // backgroud loop which applied commited entires to our cache
    private void applyCommitedEntries(){
        while(!Thread.currentThread().isInterrupted()){
            synchronized(this){
                while(lastApplied >= commitIndex){
                    try{
                        this.wait();
                    }catch(InterruptedException e) {
                        return;
                    }
                }
                lastApplied++;
                LogEntry entry = log.get(lastApplied);
                Response response = null;
                LOGGER.info("term {} node id {} applying entry {} at index {} ", currentTerm, nodeId, entry, lastApplied);
                if(!entry.isNoOp()) {
                    Command command = Command.deserialize(entry.getCommand());
                    try{
                        response = CommandProcessor.process(command, cache);
                    }catch(Exception e){
                        LOGGER.error("print stacktrace", e);

                    }

                }
                CompletableFuture<String> future = pendingRequests.remove(lastApplied);
                if(future != null && response != null) {
                    future.complete(response.toProtocolString());
                }
                else if(future != null) future.complete("SERVER_ERROR\r\n");
                LOGGER.info("term {} node id {} applied entry {} at index {} is done", currentTerm, nodeId, entry, lastApplied);
            }
        }
    }

    private synchronized void resetElectionTimer(){
        // the job of this method is to cancel election timer
        // and restart after some random time
        if(electionTimeoutFuture != null){
            electionTimeoutFuture.cancel(false);
        }
        long randomTime = ThreadLocalRandom.current().nextLong(150, 300);
        electionTimeoutFuture = scheduler.schedule(() -> {
            // start election
            startElection();
        }, randomTime, TimeUnit.MILLISECONDS);
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


}

