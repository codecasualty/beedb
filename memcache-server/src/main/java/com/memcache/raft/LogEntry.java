package com.memcache.raft;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class LogEntry {

    /*
    @param index : index of the log entry
    @param command : command to be performed on the log entry
    @param term : term of the leader
    log entry is inmmutable object , so we shouldn't change it once they are created
    requestId is used for internal tracking purposer, just like trace id in telemetry , here requestId denotes for which request this 
    logentry is created.
     */
    private final int     index;
    private final String  command;
    private final int     term;
    private final boolean noOp;
    private final String  requestId;
    @JsonCreator
    public LogEntry(@JsonProperty("index") int index, @JsonProperty("command") String command, @JsonProperty("term") int term, @JsonProperty("noOp") boolean noOp , @JsonProperty("requestId") String requestId) {
        this.index = index;
        this.command = command;
        this.term = term;
        this.noOp = noOp;
        this.requestId = requestId;
    }
    

    public int getIndex() {
        return index;
    }

    public String getCommand() {
        return command;
    }

    public int getTerm() {
        return term;
    }

    public boolean isNoOp() {
        return noOp;
    }

    public String getRequestId() {
        return requestId;
    }
    public String toString(){
        return "LogEntry{" +
                "index=" + index +
                ", command='" + command + '\'' +
                ", term=" + term +
                ", noOp=" + noOp +
                ", requestId='" + requestId + '\'' +
                '}';
    }

    public boolean equals(Object o){
        if(o instanceof LogEntry){
            LogEntry logEntry = (LogEntry) o;
            return logEntry.getIndex() == this.getIndex() && (logEntry.getCommand() != null && this.getCommand() != null ? logEntry.getCommand().equals(this.getCommand()) : true) && logEntry.getTerm() == this.getTerm() && logEntry.isNoOp() == this.isNoOp();
        }
        return false;
    }
    
}
