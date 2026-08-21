package com.memcache.raft.rpc;

/*
this class denotes the response send by the follower to the candidate to tell whether it has voted or not
 */
public class RequestVoteResponse {
    
    /*
    @param term : term of the election
    @param voteGranted : boolean value which tells whether the candidate has voted or not
    @param followerId : id of the follower
    */
    private String  followerId;
    private long     term;
    private boolean voteGranted;

    public RequestVoteResponse(String followerId, long term, boolean voteGranted) {
        this.followerId = followerId;
        this.term = term;
        this.voteGranted = voteGranted;
    }

    public RequestVoteResponse() {
    }

    public String getFollowerId() {
        return followerId;
    }

    public void setFollowerId(String followerId) {
        this.followerId = followerId;
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public void setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
    }

    public String toString() {
        return "{\"followerId\":\"" + followerId + "\",\"term\":" + term + ",\"voteGranted\":" + voteGranted + "}";
    }
}
