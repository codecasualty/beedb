package com.memcache.gateway.chaos.chaosexception;

public class NodeCouldNotBeKilled extends ChaosException{

    public NodeCouldNotBeKilled(String message) {
        super(message);
    }

    public NodeCouldNotBeKilled(String message, Throwable cause) {
        super(message, cause);
    }
    
}
