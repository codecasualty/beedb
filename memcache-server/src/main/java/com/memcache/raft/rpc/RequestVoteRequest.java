

package com.memcache.raft.rpc;

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
    private int     term;
    private int     lastLogIndex;
    private int     lastLogTerm;
    private String  candidateId;

    public RequestVoteRequest(int term, int lastLogIndex, int lastLogTerm, String candidateId) {
        this.term = term;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
        this.candidateId = candidateId;
    }

    public RequestVoteRequest() {
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public void setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
    }

    public int getLastLogTerm() {
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
}
