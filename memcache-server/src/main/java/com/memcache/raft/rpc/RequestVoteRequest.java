

package com.memcache.raft.rpc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
/*
this class denotes the request which is sent by candiate , requesting for vote thus the name is requestVote request
 */
public class RequestVoteRequest {
    /*
    @param term : term of the election
    @param lastLogIndex : index of the last log entry
    @param lastLogTerm : term of the last log entry
    @param candidateId : id of the candidate
    lastlogIndex and lastLogTerm are used to find the most recent/updated leader 
     */
    private long     term;
    private int     lastLogIndex;
    private long     lastLogTerm;
    private String  candidateId;
    private String  requestId;

    @JsonCreator
    public RequestVoteRequest(@JsonProperty("term") long term, @JsonProperty("lastLogIndex") int lastLogIndex,
    @JsonProperty("lastLogTerm") long lastLogTerm, @JsonProperty("candidateId") String candidateId , @JsonProperty String requestId) {
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.candidateId = candidateId;
        this.requestId = requestId;
    }

    public RequestVoteRequest() {
    }

    public long getTerm() {
        return term;
    }

    public void setTerm(long term) {
        this.term = term;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public long getLastLogTerm() {
        return lastLogTerm;
    }

    public void setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public void setCandidateId(String candidateId) {
        this.candidateId = candidateId;
    }

    public String getRequestId() {
        return requestId;
    }

    public String toString(){
        return "RequestVoteRequest{" + 
                "requestId "+ requestId +
                "term=" + term +
                ", lastLogIndex=" + lastLogIndex +
                ", lastLogTerm=" + lastLogTerm +
                ", candidateId='" + candidateId + '\'' +
                '}';
    }
}
