package com.memcache.handler;

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;

public class AddHandler implements CommandHandler {

    @Override
    public Response execute(Command command, Cache cache){
        String keyString = command.getKey();
        byte[] value = command.getValue();
        if(!cache.containsKey(keyString)){
            cache.put(keyString, value , command.getFlags(), command.getExpiry());
            return new Response(ResponseStatus.STORED);
        } 
        return new Response(ResponseStatus.NOT_STORED);
    }
    
}
