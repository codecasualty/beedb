package com.memcache.raft;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.memcache.cache.CacheItem;
// this will be a simple DTO , which will store last applied index and last applied term along with snapshot
public class RaftSnapshot {
    private final int                       lastAppliedIndex;
    private final int                       lastAppliedTerm;
    private final Map<String, CacheItem>    cacheState;

    @JsonCreator
    public RaftSnapshot(
        @JsonProperty("lastAppliedIndex") int lastAppliedIndex,
        @JsonProperty("lastAppliedTerm") int lastAppliedTerm,
        @JsonProperty("cacheState") Map<String, CacheItem> cacheState
    ){
        this.lastAppliedIndex = lastAppliedIndex;
        this.lastAppliedTerm  = lastAppliedTerm;
        this.cacheState       = cacheState;
    }

    public int getLastAppliedIndex(){
        return lastAppliedIndex;
    }


    public int getLastAppliedTerm(){
        return lastAppliedTerm;
    }

    public Map<String, CacheItem> getCacheState(){
        return cacheState;
    }
}