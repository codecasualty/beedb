package com.memcache;

import com.memcache.response.Response;
import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.command.CommandParser;
import com.memcache.handler.CommandProcessor;

public class ConnectionHandler {

    public static String handle(String commandLine, byte[] value, Cache cache) throws Exception{
        Command command = CommandParser.parse(commandLine);
        if(command.getByteLength() >= 0) command.setValue(value);
        Response response = CommandProcessor.process(command, cache);
        return response.toProtocolString();
    }
    
}
