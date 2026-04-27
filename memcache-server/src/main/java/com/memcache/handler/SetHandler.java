package com.memcache.handler;
import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;

public class SetHandler implements CommandHandler {
    
    @Override
    public Response execute(Command command, Cache cache) {
        cache.put(command.getKey(), command.getValue(), command.getFlags(), command.getExpiry());
        return new Response(ResponseStatus.STORED);
    }
}
