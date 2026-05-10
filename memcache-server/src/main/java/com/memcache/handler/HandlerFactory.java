package com.memcache.handler;

import com.memcache.command.CommandType;

public class HandlerFactory {
    
    public static CommandHandler getHandler(CommandType command){
        
        switch(command){
            case GET:
                return new GetHandler();
            case SET:
                return new SetHandler();
            case ADD:
                return new AddHandler();
            case REPLACE:
                return new ReplaceHandler();
            case APPEND:
                return new AppendHandler();
            case PREPEND:
                return new PrependHandler();
            default:
                return null;
        }
    }
}
