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
