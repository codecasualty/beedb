package com.memcache.handler;

import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;
import com.memcache.cache.Cache;

public class CommandProcessor {

    public static Response process(Command command, Cache cache) throws Exception{

        CommandHandler handler = HandlerFactory.getHandler(command.getType());
        if(handler == null) return new Response(ResponseStatus.ERROR);
        return handler.execute(command, cache);
    }
    
}
