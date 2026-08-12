
package com.memcache.raft.wal;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.memcache.raft.LogEntry;

public class WalRecord {

    EntryType entryType;
    LogEntry logEntry;
    int fromIndex;
    @JsonCreator
    public WalRecord(
        @JsonProperty("entryType") EntryType entryType,
        @JsonProperty("logEntry") LogEntry logEntry,
        @JsonProperty("fromIndex") int fromIndex
    ){
        this.entryType = entryType;
        this.logEntry = logEntry;
        this.fromIndex = fromIndex;
    }

    public EntryType getEntryType(){
        return this.entryType;
    }

    public LogEntry getLogEntry(){
        return logEntry;
    }

    public int getFromIndex(){
        return fromIndex;
    }

    public String toString(){
        return "WalRecord{" +
                "entryType=" + entryType +
                ", logEntry=" + logEntry +
                ", fromIndex=" + fromIndex +
                '}';
    }
    
}
