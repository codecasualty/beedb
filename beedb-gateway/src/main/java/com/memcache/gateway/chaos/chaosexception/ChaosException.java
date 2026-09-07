package com.memcache.gateway.chaos.chaosexception;

public class ChaosException extends Exception{

    public ChaosException(String message) {
        super(message);
    }

    public ChaosException(String message, Throwable cause) {
        super(message, cause);
    }
    
}
