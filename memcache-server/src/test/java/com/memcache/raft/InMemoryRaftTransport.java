package com.memcache.raft;

import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.rpc.AppendEntriesResponse;

import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InMemoryRaftTransport implements RaftTransport {
    
    // here the idea is we will store raftnode info in this map so that we dont need to create explicit raft node for each peer
    // and communication among them will be handled by our in memory raft node
    private Map<String, RaftNode> raftNodes ;
    private Logger LOGGER = LoggerFactory.getLogger(InMemoryRaftTransport.class.getName());

    public InMemoryRaftTransport() {
        this.raftNodes = new HashMap<>();
    }

    public void addRaftNode(String nodeIP , RaftNode raftNode){
        raftNodes.put(nodeIP, raftNode);
    }

    public void removeRaftNode(String nodeIP){
        raftNodes.remove(nodeIP);
    }

    @Override
    public RequestVoteResponse sendRequestVoteToPeer(RequestVoteRequest request, String peer) {
        LOGGER.info("Vote Request by node {} to peer{} request is {} ", request.getCandidateId() , peer, request);
        RaftNode raftNode = raftNodes.get(peer);
        if(raftNode == null) return null;
        return raftNode.handleRequestVote(request);
    }

    @Override
    public AppendEntriesResponse sendAppendEntriesToPeer(AppendEntriesRequest request, String peer) {
        LOGGER.info("Append Entry Req by node {} to peer{} request is {} ", request.getLeaderId() , peer, request);
        RaftNode raftNode = raftNodes.get(peer);
        if(raftNode == null) return null;
        return raftNode.handleAppendEntries(request);
    }
}
