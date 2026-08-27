package com.memcache.handler;

import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
import com.memcache.command.Command;
import com.memcache.response.Response;
import com.memcache.response.ResponseStatus;

public class GetHandler  implements CommandHandler {
    
    @Override
    public Response execute(Command command, Cache cache) {
        CacheItem item = cache.get(command.getKey());
        Response response = new Response(ResponseStatus.END);
        response.addItem(item);
        return response;
    }
}
