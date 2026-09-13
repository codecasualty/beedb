package com.memcache.gateway.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import java.io.IOException;
import com.memcache.gateway.client.beedbexception.BeedbException;
import com.memcache.gateway.client.beedbexception.BeedbTimeoutException;
import com.memcache.gateway.client.beedbexception.NoLeaderException;
import com.memcache.gateway.client.beedbexception.NodeUnreachableException;

@RestControllerAdvice
public class ApiExceptionHandler {
    
    /*
    * This method is called when an exception occurs in the controller.
    * It is used to return a custom error message to the client.
    *
    * @param ex - The exception that occurred.
    * @return - The error message to be returned to the client.
     */

    private static final Logger LOGGER = LoggerFactory.getLogger(ApiExceptionHandler.class);

    @ExceptionHandler (BeedbException.class)
    public ResponseEntity<String> handleException(BeedbException ex){
        if(ex instanceof NodeUnreachableException){
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "1").body("Please retry after 1 seconds");
        }
        else if(ex instanceof NoLeaderException){
            return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).header("Retry-After", "2").body("Election is in 2 seconds");
        }
        else if(ex instanceof BeedbTimeoutException){
            return ResponseEntity.status(HttpStatus.GATEWAY_TIMEOUT).body("Gateway TimedOut");
        }
        LOGGER.error("Exception occurred in the controller", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(ex.getMessage());
    }

    @ExceptionHandler (IOException.class)
    public void handleClientDisconnect(IOException ex){
        LOGGER.debug("client disconnected: {}", ex.getMessage());
    }
}
