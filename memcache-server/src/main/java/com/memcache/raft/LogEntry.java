package com.memcache.raft;

public class LogEntry {

    /*
    @param index : index of the log entry
    @param command : command to be performed on the log entry
    @param term : term of the leader
    log entry is inmmutable object , so we shouldn't change it once they are created
     */
    private final int     index;
    private final String  command;
    private final int     term;
    private final boolean noOp;

    public LogEntry(int index, String command, int term, boolean noOp){
        this.index = index;
        this.command = command;
        this.term = term;
        this.noOp = noOp;
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
    
}
