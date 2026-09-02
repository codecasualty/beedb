package com.memcache.gateway.client.beedbexception;

public class BeedbTimeoutException extends BeedbException {

    public BeedbTimeoutException(String message) {
        super(message);
    }

    public BeedbTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
