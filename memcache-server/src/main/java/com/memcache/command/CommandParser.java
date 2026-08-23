package com.memcache.command;

public class CommandParser {

    public static Command parse(String line) throws Exception{
        
        String[] parts = line.trim().split("\\s+");
        if(parts.length < 2) throw new Exception("Invalid command");
        CommandType type = null;
        try{
            type = CommandType.valueOf(parts[0].toUpperCase());
        }catch(Exception e){
            throw new Exception("Invalid command");
        }
        if(type == null) throw new Exception("Invalid command");

        switch(type){
            case SET:
            case ADD:
            case REPLACE:
            case APPEND:
            case PREPEND:
                return parseStorageCommand(parts, type);
            case DELETE:
            case GET:
                return parseRetrievalCommand(parts, type);
            default:
                throw new Exception("Invalid command");
        }

    }

    private static Command parseStorageCommand(String[] parts, CommandType type){
        String key = parts[1];
        int flags = Integer.parseInt(parts[2]);
        int expiry = Integer.parseInt(parts[3]);
        int byteLength = Integer.parseInt(parts[4]);
        return new Command(type, key, flags, expiry, byteLength);
    }

    private static Command parseRetrievalCommand(String[] parts, CommandType type){
        String key = parts[1];
        return new Command(type, key, 0, 0, -1);
    }
    
}
/*
what is log entry ?
according to me , its the entry in log which will have info about the command, like the term it was issued in, the index it was issued at and the command itself.
to know where a particular log entry lives we need to know log index and term and using that we can find the log entry.
we will also need prevLogIndex and prevLogTerm to know where the entry came from. 
we will also need commit index and applied index to know where the entry is in the state machine. applied index denotes entries which are applied on state machine and our datastore
whereas commit index denotes the entries which are committed by majority of nodes, it may or may not be applied on datastore.
the command which we are storing need be of either heartbeat , or append entries or no-op entry. so that a clear distinction can be made between them.
so summarizing it
command : {heartbeat, append entries, no-op entry}
log entry : {term, index, command} an object of this type will be stored in log
prevLogIndex, prevLogTerm, commitIndex, appliedIndex each of type int

an append entry is a command which is appended to the log
a term to denote which term is it
a leader id to denote from which leader we have received the log
the commit index to denote how many entries have been committed by the leader
log entries to denote which entries this current node needs to apply to its log
prevLogIndex which denotes , according to leader, you have entries till this index
prevLogTerm which denotes, according to leader, you had these many entries in this x term

a raft log is datastore which stores all the log entries
so it must have list<logEntry>
also if its leader then it must have nextIndex[] to send and , index commited by those nodes commitIndex[]
what if that node is not leader then it must have
leaderId
commitIndex , 
appliedIndex
term

to read the entries from the log to send to nodes then we will need log.get(nextIndex[nodeId]) till log.get(log.size()-1)
to find prevLogTerm of an entry we figure out that index and in log , we find out that log entry and in that entry we have term.
to find out conflicts on follower, a follower will first need to check if it has that entry on given index by leader, if not then it will apply that entry to its datastore 
else if it has some entry in that index then it will check the term of that entry , if its greater/lesser than the term of what is sent by leader then it will sent a negative ack to leader,
asking it to send the previous entry and this will keep on continuing until it find the entry which has same index and term as the one sent by leader.

 */