package com.memcache.handler;
import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.response.Response;

public interface CommandHandler {
    Response execute(Command command, Cache cache);
}
