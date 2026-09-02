
package com.memcache.gateway.client.beedbexception;

public class NodeUnreachableException extends BeedbException {

    public NodeUnreachableException(String message) {
        super(message);
    }
    
    public NodeUnreachableException(String message, Throwable cause) {
        super(message, cause);
    }
}