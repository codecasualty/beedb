package com.memcache.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/*
this class denotes the response send by the follower to the leader to tell whether it has appended the entries or not
 */
public class AppendEntriesResponse {
    /*
    @param term : term of the election
    @param success : boolean value which tells whether the follower has appended the entries or not
    @param followerId : id of the follower
    @param matchIndex : index which is confirmed replicated in peers log, not necessary to be applied in state machine.
     */
    private boolean success;
    private int     term;
    private String  followerId;
    private int     matchIndex;

    @JsonCreator
    public AppendEntriesResponse(@JsonProperty("success") boolean success,@JsonProperty("term") int term,@JsonProperty("followerId") String followerId,@JsonProperty("matchIndex") int matchIndex) {
        this.success = success;
        this.term = term;
        this.followerId = followerId;
        this.matchIndex = matchIndex;
    }

    public AppendEntriesResponse() {
    }

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getFollowerId() {
        return followerId;
    }

    public void setFollowerId(String followerId) {
        this.followerId = followerId;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public void setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
    }
}
