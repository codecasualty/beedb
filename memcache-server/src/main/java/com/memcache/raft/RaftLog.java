package com.memcache.raft;

import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;

public class RaftLog {
    
    private List<LogEntry> logEntries;
    private Logger LOGGER = LoggerFactory.getLogger(RaftLog.class.getName());
    // these two vars denote, how many entries and for which term we have snapshotted
    private int lastIncludedIndex;
    private int lastIncludedTerm;

    public RaftLog(List<LogEntry> log) {
        this.logEntries = log;
    }

    public RaftLog(){
        logEntries = new ArrayList<LogEntry>();
        // adding a sentenel entry to log
        logEntries.add(new LogEntry(0, null, 0, true));
    }

    public RaftLog(int lastIncludedIndex , int lastIncludedTerm){
        logEntries = new ArrayList<LogEntry>();
        logEntries.add(new LogEntry(lastIncludedIndex, null, lastIncludedTerm, true));
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
        logEntries.subList(index - lastIncludedIndex, logEntries.size()).clear();
    }

    public List<LogEntry> getFrom(int fromIndex){
        return new ArrayList<LogEntry>(logEntries.subList(fromIndex - lastIncludedIndex, logEntries.size()));
    }

    // this is for vote restriction , we want to make sure that we are electing the competent leader
    public boolean isUpToDate(int index, int term){
        LOGGER.info("index {} term {} & current lastindex {} lastterm {} ", index, term, lastIndex(), lastTerm());
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
        // let say we have take snapshot of entries till lastincluded index that means those entries after taking snapshot
        // must've been removed from log , so we can compare them and if we have take snapshot there is no need to compare them as we
        // already have it
        if(lastIncludedIndex > index) return true;
        // now lets say leader has sent index 80 and term 5 and we have index 80 but term 3, so its important to check term , if we directly
        // return true then we will be appending these entries which are wrong , we have to check term if they do not match then we have to ask
        // leader for appropriate entries.
        else if(lastIncludedIndex == index) return lastIncludedTerm == term;
        return index <= lastIndex() && termAt(index) == term;
    }

    public void append(LogEntry entry){
        logEntries.add(entry);
    }

    public LogEntry get(int index){
        // lastincludedindex is offset, because we may have removed these many entries from our log during snapshotting and compaction
        return logEntries.get(index - lastIncludedIndex);
    }

    public int termAt(int index){
        return logEntries.get(index - lastIncludedIndex).getTerm();
    }

    public int size(){
        return logEntries.size();
    }

    public void setLastIncludedIndex(int index){
        this.lastIncludedIndex = index;
    }

    public void setLastIncludedTerm(int term){
        this.lastIncludedTerm = term;
    }

    public void compactTill(int index){
        logEntries = new ArrayList<>(logEntries.subList(index - lastIncludedIndex , logEntries.size()));
    }

    public void appendAll(List<LogEntry> entries){
        logEntries.addAll(entries);
    }

}
