package com.memcache.raft;

import java.util.List;
import java.util.ArrayList;

public class RaftLog {
    
    private List<LogEntry> logEntries;

    public RaftLog(List<LogEntry> log) {
        this.logEntries = log;
    }

    public RaftLog(){
        logEntries = new ArrayList<LogEntry>();
        // adding a sentenel entry to log
        logEntries.add(new LogEntry(0, null, 0 , true));
    }

    public int lastIndex(){
        return logEntries.get(logEntries.size() - 1).getIndex();
    }

    public int lastTerm(){
        return logEntries.get(logEntries.size() - 1).getTerm();
    }

    public void truncateFrom(int index){
        // here we are using sublist to truncate the log entries, because its memory efficient , avoid copying data 
        // this means from index to end of list we will remove all the entries  
        // because subList returns a view of the list we will need to remove/clear out the entries from the original list
        logEntries.subList(index, logEntries.size()).clear();
    }

    public List<LogEntry> getFrom(int fromIndex){
        return new ArrayList<LogEntry>(logEntries.subList(fromIndex, logEntries.size()));
    }

    // this is for vote restriction , we want to make sure that we are electing the competent leader
    public boolean isUpToDate(int index, int term){
        LogEntry lastLog = logEntries.get(logEntries.size() - 1);
        int currentTerm = lastLog.getTerm();
        int currentIndex = lastLog.getIndex();
        if(currentTerm < term) return true;
        else if(currentTerm > term) return false;
        else if(currentIndex <= index) return true;
        return false;
    }

    // this is for checking if we are accepting this append entry from leader or not ,
    // we want to make sure that we are not accepting stale/wrong entries, if leader sends some entry at particular index
    // and lets say we have entry at that index and its not the same as what leader is sending then we will reject that entry
    public boolean hasMatchAt(int index, int term){
        return index <= lastIndex() && termAt(index) == term;
    }

    public void append(LogEntry entry){
        logEntries.add(entry);
    }

    public LogEntry get(int index){
        return logEntries.get(index);
    }

    public int termAt(int index){
        return logEntries.get(index).getTerm();
    }

    public int size(){
        return logEntries.size();
    }

}
