package com.memcache.handler;

import com.memcache.command.CommandType;

public class HandlerFactory {
    
    public static CommandHandler getHandler(CommandType command){
        
        switch(command){
            case GET:
                return new GetHandler();
            case SET:
                return new SetHandler();
            // case ADD:
            //     return new AddHandler();
            // case REPLACE:
            //     return new ReplaceHandler();
            // case DELETE:
            //     return new DeleteHandler();
            // case INCREMENT:
            //     return new IncrementHandler();
            // case DECREMENT:
            //     return new DecrementHandler();
            // case APPEND:
            //     return new AppendHandler();
            // case PREPEND:
            //     return new PrependHandler();
            // case TOUCH:
            default:
                return null;
        }
    }
}
