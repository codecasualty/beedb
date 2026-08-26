package com.memcache.handler;


import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;

public class DeleteHandler implements CommandHandler {

    @Override
    public Response execute(Command command, Cache cache) {
        boolean result = cache.remove(command.getKey());
        if (result == false) return new Response(ResponseStatus.NOT_FOUND);
        Response response = new Response(ResponseStatus.DELETED);
        return response;
    }

    
}
