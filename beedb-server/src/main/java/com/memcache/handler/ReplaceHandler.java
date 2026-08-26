package com.memcache.handler;

import com.memcache.cache.Cache;
import com.memcache.response.ResponseStatus;
import com.memcache.command.Command;
import com.memcache.response.Response;

public class ReplaceHandler implements CommandHandler {

    @Override
    public Response execute(Command command, Cache cache) {

        String keyString = command.getKey();
        if(cache.containsKey(keyString)){
            cache.put(keyString, command.getValue(), command.getFlags(), command.getExpiry());
            return new Response(ResponseStatus.STORED);
        }
        return new Response(ResponseStatus.NOT_STORED);
        
    }
    
}
