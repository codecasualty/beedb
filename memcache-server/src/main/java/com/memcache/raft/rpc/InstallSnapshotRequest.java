package com.memcache.raft.rpc;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.memcache.cache.CacheItem;
/*
 * This class is Request DTO sent by leader to followers to install snapshot
 * the mechanism is as follows:
 * 1. leader sends InstallSnapshotRequest to followers
 * 2. followers send InstallSnapshotResponse to leader 
 * Its pushed to followers by leader therefore its request by leader to followers
 */
public class InstallSnapshotRequest {
    
    private int     term;
    private String  leaderId;
    private int     lastIncludedIndex;
    private int     lastIncludedTerm;
    private Map<String , CacheItem> cacheState;

    @JsonCreator
    public InstallSnapshotRequest(
        @JsonProperty("term") int term,
        @JsonProperty("leaderId") String leaderId,
        @JsonProperty("lastIncludedIndex") int lastIncludedIndex,
        @JsonProperty("lastIncludedTerm") int lastIncludedTerm,
        @JsonProperty("cacheState") Map<String , CacheItem> cacheState
    ){
        this.term = term;
        this.leaderId = leaderId;
        this.lastIncludedIndex = lastIncludedIndex;
        this.lastIncludedTerm = lastIncludedTerm;
        this.cacheState = cacheState;
    }

    public int getTerm() {
        return term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public int getLastIncludedIndex() {
        return lastIncludedIndex;
    }

    public int getLastIncludedTerm() {
        return lastIncludedTerm;
    }

    public Map<String , CacheItem> getCacheState() {
        return cacheState;
    }

    public String toString() {
        return "InstallSnapshotRequest [term=" + term + ", leaderId=" + leaderId + ", lastIncludedIndex=" + lastIncludedIndex + ", lastIncludedTerm=" + lastIncludedTerm + ", cacheState=" + printCacheState() + "]";
    }
    
    private String printCacheState() {
        StringBuilder sb = new StringBuilder();
        for(String key : cacheState.keySet()) {
            sb.append(key).append("=").append(cacheState.get(key)).append("\n");
        }
        return sb.toString();
    }
}
