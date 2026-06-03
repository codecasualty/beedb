package com.memcache.raft;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
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

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
public class RaftNode {
    
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
    private       ScheduledFuture<?>       heartbeatTimeoutFuture;
    // for network communication
    private final ExecutorService          rpcExecutor;
    // for applying the log, when we receives response from followers we apply the log and update the nextindex and match index and commit logs as well
    // so for each follower we have a virtual threads whose job is to replication  
    private final ExecutorService          applyExecutor;

    // external dependencies
    private List<String>     peerAddresses;
    private String          nodeId;
    private Cache           cache;


    public RaftNode(List<String> peerAddresses, String nodeId, Cache cache){
        this.peerAddresses = peerAddresses;
        this.nodeId = nodeId;
        this.cache = cache;
        this.log = new RaftLog();
        this.peers = new HashMap<>();
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.rpcExecutor = Executors.newVirtualThreadPerTaskExecutor();
        this.applyExecutor = Executors.newSingleThreadExecutor();
        this.role = NodeRole.FOLLOWER;
        //  a timer to wait for heartbeat from leader
        resetElectionTimer();
        this.applyExecutor.submit(this::applyCommitedEntries);
    }
    
    private synchronized void startElection(){

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
        RequestVoteRequest request = buildReqestVoteRequest();
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
        List<Future<RequestVoteResponse>> futures = peerAddresses.stream()
        .map(peer -> rpcExecutor.submit(() -> {
            return sendRequestVoteToPeer(peer, request);
        })).collect(Collectors.toList());
        List<RequestVoteResponse> responses = new ArrayList<>();
        for(Future<RequestVoteResponse> future: futures){
            try{
                RequestVoteResponse response = future.get(150, TimeUnit.MILLISECONDS);
                if(response != null)
                    responses.add(response);
            }catch(Exception e){
                // not able to get response from other nodes, either peer unreachable or timeout
                e.printStackTrace();
            }
        }
        return responses;
    }

    private RequestVoteResponse sendRequestVoteToPeer(String peer, RequestVoteRequest request){
        return null;
    }

    private void stepDownDueToHigherTerm(int term){
        currentTerm = term;
        votedFor = null;
        transitionToFollower();
        resetElectionTimer();
        cancelHeartbeatTimer();
    }
    private RequestVoteRequest buildReqestVoteRequest(){
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
        scheduleHeartbeat();
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
        while(isLeader()){
            
            final AppendEntriesRequest request = getAppendEntriesRequest(peer);
            AppendEntriesResponse response = sendAppendEntriesToPeerInParallel(request , peer);
            if(response == null) continue;
            // we are adding sleep because , if we dont have anything else to replicate, withoout sleep we will continously replicate emtpy entries
            // which will flood the system , also our while loop is tight spin , meaning it will keep on sending heartbeats/appendentries continously
            if(response.isSuccess() && request.getEntries().isEmpty()){
                try{
                    Thread.sleep(100);
                }catch(InterruptedException e){
                    e.printStackTrace();
                }
            }
            
            synchronized(this){
                if(response.getTerm() > currentTerm){
                    stepDownDueToHigherTerm(response.getTerm());
                    return;
                }
                updatePeerState(response);
                mayBeAdvanceCommitIndex();
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
            // to wake up apply thread ,as we have moved our commit index, therefore rest of entries should be applied to cache.
            this.notifyAll();
        }
    }   

    private LogEntry getNoOpEntry(){
        LogEntry noOp = new LogEntry(log.lastIndex() + 1, null , currentTerm ,true);
        return noOp;
    }

    private void scheduleHeartbeat(){
        // we need to send heartbeats to all peers
        // send heratbeats every 50 ms
        heartbeatTimeoutFuture = scheduler.scheduleAtFixedRate(() -> {
            // start election
            sendHeartbeats();
        }, 0,50, TimeUnit.MILLISECONDS);
    }

    private void cancelHeartbeatTimer(){
        if(heartbeatTimeoutFuture != null){
            heartbeatTimeoutFuture.cancel(false);
            heartbeatTimeoutFuture = null;
        }
    }

    private synchronized AppendEntriesRequest getAppendEntriesRequest(String peer){
        int prevIndex = peers.get(peer).getNextIndex() - 1;
        int prevTerm = log.termAt(prevIndex);
        List<LogEntry> list = log.getFrom(prevIndex + 1);
        // because leaderId can be null or stale values so its better touse nodeId
        return new AppendEntriesRequest(currentTerm , nodeId , prevIndex, prevTerm, commitIndex , list);
    }
    // its importatnt to use sychronized keyword in updatedpeerstate because if we dont use 
    // we may think that all we are doing is modifying result or entry of only the follower which is present in response, but we should forget that 
    // our map (peers) is not concurrent map , that means, if multiple threads tries to read from it even though different entries or keys , it might misbehave
    // so its better to use concurrent map or use synchronized methods to avoid race conditions.
    // for example one thread trying to read some other entry in this updatepeerstate , but some other thread in some other method trying to read that same entry both can have
    // inconsistent info/result available which would be difficult to trace without proper synchronization mechanism
    private synchronized void updatePeerState(AppendEntriesResponse response){
        String followerId = response.getFollowerId();
        int nextMatchIndex = response.getMatchIndex();
        boolean success  = response.isSuccess();
        PeerState state = peers.get(followerId);
        state.setMatchIndex(nextMatchIndex);
        if(success) state.setNextIndex(state.getNextIndex() + 1);
        else if(state.getNextIndex() > 1 ) state.setNextIndex(state.getNextIndex() - 1);

    }
    // this method shouldn't be synchronized because we are using virtual threads to replicate the log and we dont want to block the main thread
    private  AppendEntriesResponse sendAppendEntriesToPeerInParallel(AppendEntriesRequest request, String peer){
        return null;
    }
    private synchronized void sendHeartbeats(){
    }

    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest requestVoteRequest){
        // some node is asking for vote
        // Seeing a higher term means you must update your term. It does not mean you must grant the vote.
        // lets say a node which is asking us for vote has term x > our current term, what does this mean 
        //  it means it has gone through more election timeouts and has seens more terms but that does not imply that it has more updated logs
        // becaues it can happen that in cluster of 5 nodes , due to partition a group of nodes can keep on particicpating in election becaues they are not able to reach
        // to majority thus just having more/larger term does not gurantee that the node is supposed to be leader, but it does mean that the current node was not part of all those
        // terms and there fore its better to step down and let all nodes agree on this current term and then continue the leader election process.

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
                return response;
            }
        }
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
        
        AppendEntriesResponse  response = builAppendEntriesResponse();
        if(request.getTerm() < currentTerm){
            return response;
        }
        // we cannot be leader if node who is asking for appending entries has more term than us
        if(request.getTerm() > currentTerm){
            stepDownDueToHigherTerm(request.getTerm());
        }

        if(role == NodeRole.LEADER)cancelHeartbeatTimer();
        transitionToFollower();

        leaderId = request.getLeaderId();
        resetElectionTimer();
        // whether the node which is asking for appending entry has updated log 
        if(!log.hasMatchAt(request.getPrevLogIndex() , request.getPrevLogTerm())){
            response.setSuccess(false);
            return response;
        }
        // if the node which is asking us to append entry has updated log then we can safely append it in our log
        log.truncateFrom(request.getPrevLogIndex() + 1);
        for(LogEntry entry : request.getEntries()) if(!entry.isNoOp())log.append(entry);

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
    public CompletableFuture<String> propose(String command){
        return null;
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
                Command command = Command.deserialize(entry.getCommand());
                cache.put(command.getKey(), command.getValue(), command.getFlags(), command.getExpiry());
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

}

