package com.memcache.raft.rpc;
import java.util.List;

import com.memcache.raft.LogEntry;
/*
this class denotes the append entry request send by leader to follower
 */
public class AppendEntriesRequest {

    private int         term;
    private String      leaderId;
    private int         prevLogIndex;
    private int         prevLogTerm;
    private int         leaderCommit;
    private List<LogEntry> entries;

    public AppendEntriesRequest(int term, String leaderId, int prevLogIndex, int prevLogTerm, int leaderCommit, List<LogEntry> entries) {
        this.term = term;
        this.leaderId = leaderId;
        this.prevLogIndex = prevLogIndex;
        this.prevLogTerm = prevLogTerm;
        this.leaderCommit = leaderCommit;
        this.entries = entries;
    }

    public AppendEntriesRequest() {
    }

    public int getTerm() {
        return term;
    }

    public void setTerm(int term) {
        this.term = term;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public void setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public void setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public void setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }
    
    public void setEntries(List<LogEntry> entries) {
        this.entries = entries;
    }
}
