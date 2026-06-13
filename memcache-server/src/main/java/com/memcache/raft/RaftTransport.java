package com.memcache.raft;

import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.rpc.AppendEntriesResponse;

public interface RaftTransport {
    RequestVoteResponse sendRequestVoteToPeer(RequestVoteRequest request, String peer);
    AppendEntriesResponse sendAppendEntriesToPeer(AppendEntriesRequest request, String peer);
}
