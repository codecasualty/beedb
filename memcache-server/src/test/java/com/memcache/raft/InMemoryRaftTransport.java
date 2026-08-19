package com.memcache.raft;

import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;

import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

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
        Map<String , String> saved = MDC.getCopyOfContextMap();
        LOGGER.info("Request Vote Req by node {} to peer{} request is {} ", request.getCandidateId() , peer, request);
        RaftNode raftNode = raftNodes.get(peer);
        try{
            if(raftNode == null) return null;
            return raftNode.handleRequestVote(request);
        }finally{
            if(saved != null) MDC.setContextMap(saved);
            else MDC.clear();
        }
    }
     

    @Override
    public AppendEntriesResponse sendAppendEntriesToPeer(AppendEntriesRequest request, String peer) {
        Map<String , String> saved = MDC.getCopyOfContextMap();
        LOGGER.info("Append Entry Req by node {} to peer{} request is {} ", request.getLeaderId() , peer, request);
        RaftNode raftNode = raftNodes.get(peer);
        try{
            if(raftNode == null) return null;
            return raftNode.handleAppendEntries(request);
        }finally{
            if(saved != null) MDC.setContextMap(saved);
            else MDC.clear();
        }
    }

    @Override
    public InstallSnapshotResponse sendInstallSnapshotToPeer(InstallSnapshotRequest request, String peer) {
        Map<String , String> saved = MDC.getCopyOfContextMap();
        LOGGER.info("Install Snapshot Req by node {} to peer{} request is {} ", request.getLeaderId() , peer, request);
        RaftNode raftNode = raftNodes.get(peer);
        try{
            if(raftNode == null) return null;
            return raftNode.handleInstallSnapshot(request);
        }finally{
            if(saved != null) MDC.setContextMap(saved);
            else MDC.clear();
        }
    }
}
