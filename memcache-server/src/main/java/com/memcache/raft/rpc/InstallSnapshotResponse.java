
package com.memcache.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class InstallSnapshotResponse {
    
    private int term;
    private String followerId;
    private int    appliedIndex;
    private boolean success; 

    @JsonCreator
    public InstallSnapshotResponse(
        @JsonProperty("term") int term,
        @JsonProperty("followerId") String followerId,
        @JsonProperty("appliedIndex") int appliedIndex,
        @JsonProperty("success") boolean success
    ){
        this.term = term;
        this.followerId = followerId;
        this.appliedIndex = appliedIndex;
        this.success = success;
    }

    public InstallSnapshotResponse() {
    }

    public int getTerm() {
        return term;
    }

    public String getFollowerId() {
        return followerId;
    }

    public int getAppliedIndex() {
        return appliedIndex;
    }

    public boolean getSuccess() {
        return success;
    }   

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public void setFollowerId(String followerId) {
        this.followerId = followerId;
    }

    public void setAppliedIndex(int appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    public String toString() {
        return "InstallSnapshotResponse [term=" + term + ", followerId=" + followerId + ", appliedIndex=" + appliedIndex + ", success=" + success + "]";
    }



}
