package com.memcache.raft;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
public class RaftState {
    private final int    term;
    private final String votedFor;

    @JsonCreator
    public RaftState(
        @JsonProperty("term") int currentTerm,
        @JsonProperty("votedFor") String votedFor
    ){
        this.term = currentTerm;
        this.votedFor = votedFor;
    }

    public int getTerm(){
        return term;
    }

    public String getVotedFor(){
        return votedFor;
    }

    public String toString(){
        return "RaftState{" +
            "term=" + term +
            ", votedFor='" + votedFor + '\'' +
            '}';
    }
}
