package com.memcache.gateway.client.beedbexception;

public class NoLeaderException extends BeedbException {

    public NoLeaderException(String message) {
        super(message);
    }

    public NoLeaderException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
