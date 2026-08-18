package com.memcache.raft;

import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;

public interface RaftTransport {
    RequestVoteResponse sendRequestVoteToPeer(RequestVoteRequest request, String peer);
    AppendEntriesResponse sendAppendEntriesToPeer(AppendEntriesRequest request, String peer);
    InstallSnapshotResponse sendInstallSnapshotToPeer(InstallSnapshotRequest request, String peer);
}
