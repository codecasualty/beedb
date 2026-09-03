/*
 * Copyright 2026 subodh.
 * this class opens a socket connection to local host : 11211 port where beedb-server is running
 * and then sends commands to beedb-server
 */
package com.memcache.gateway.client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.gateway.client.beedbexception.BeedbException;
import com.memcache.gateway.client.beedbexception.BeedbTimeoutException;
import com.memcache.gateway.client.beedbexception.NoLeaderException;
import com.memcache.gateway.client.beedbexception.NodeUnreachableException;

import jakarta.annotation.PreDestroy;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.Map;
public class BeedbClient {

    private final ConnectionPool pool;
    private final Map<String, String> nodes; // "node1" -> "127.0.0.1:11211"
    private volatile String leaderAddressString; // an address 
    private Logger LOGGER = LoggerFactory.getLogger(BeedbClient.class.getName());

    public BeedbClient(Map<String, String> nodes) {
        this.nodes = nodes;
        this.pool = new ConnectionPool();
        this.leaderAddressString = "";
    }

    public String get(String key){
        return execute("get", connection -> connection.getValue(key));
    }

    public String set(String key, String value){
        return execute("set", connection -> connection.setValue(key, value));
    }

    public String delete(String key){
        return execute("delete", connection -> connection.deleteValue(key));
    }


    public String getStats(String address){
        String response = null;
        BeedbConnection connection = null;
        try{
            connection = pool.getConnection(address);
            response = connection.getStats();
            pool.putConnection(address, connection);
        }catch(Exception e){
            logFailure("stats", address, e);
            pool.closeConnection(connection);
        }
        return response;
    }

    public String getleaderAddressString() throws NoLeaderException{
        String response = null;
        String leaderAdd = null;
        int retryCount = 0;
        while(retryCount++ < 3){
            for(String addressSet : nodes.values()){
                response = getStats(addressSet);
                if(response == null)continue;
                String[] lines = response.split("\n");
                String prefix = "STAT raft_leader_id ";
                for (String line : lines) {
                    if (line.startsWith(prefix)) {
                        leaderAdd = line.substring(prefix.length());
                        break;
                    }
                }
            }
            if(leaderAdd != null)break;
            try{
                Thread.sleep(1000);
            }catch(Exception e){
                Thread.currentThread().interrupt();
                LOGGER.warn("interrupted while waiting to re-resolve the leader");
                break;
            }   
        }
        if(leaderAdd != null) leaderAddressString = nodes.get(leaderAdd);
        if(leaderAdd == null) throw new NoLeaderException("no leader found in the cluster" );
        else return leaderAddressString;
    }

    


    /**
     *
     * A ConnectException to a node that has just died, or a read timeout under load,
     * is EXPECTED -- we already know why it happened, so a stack trace adds nothing
     * and costs a lot.
     *
     * Anything else is genuinely unknown, and there the trace is the only thing that
     * tells you what happened -- so that one keeps ERROR + trace.
     *
     * In SLF4J a Throwable passed as the LAST argument with no matching {} is what
     * triggers the stack trace; passing e.getMessage() instead keeps it to one line.
     */
    private void logFailure(String operation, String address, Exception e){
        if(e instanceof java.net.ConnectException || e instanceof java.net.SocketException){
            LOGGER.warn("{} failed: node {} is unreachable ({})", operation, address, e.getMessage());
        }else if(e instanceof java.net.SocketTimeoutException){
            LOGGER.warn("{} failed: node {} timed out ({})", operation, address, e.getMessage());
        }else{
            LOGGER.error("{} failed unexpectedly against node {}", operation, address, e);
        }
    }

    private String execute(String operation, BeedbOperation operationFunction){
        BeedbConnection connection = null;
        String response = null;
        try{
            if(leaderAddressString == null || leaderAddressString.isEmpty()){
                leaderAddressString = getleaderAddressString();
            }
            connection = pool.getConnection(leaderAddressString);
            response = operationFunction.apply(connection);
            if(response != null && response.startsWith("SERVER_ERROR")){
                pool.putConnection(leaderAddressString, connection);
                leaderAddressString = getleaderAddressString();
                connection = pool.getConnection(leaderAddressString);
                response = operationFunction.apply(connection);
            }
            pool.putConnection(leaderAddressString, connection);
            
        }catch(Exception e){
            logFailure(operation, leaderAddressString, e);
            // A connection-level failure means this node may no longer be the leader
            // (or may be gone). Drop the cached address so the next call re-resolves;
            // without this the client dials a dead node forever.
            String leaderAdd = leaderAddressString;
            leaderAddressString = null;
            pool.closeConnection(connection);
            throw translate(operation , leaderAdd , e);
        }
        
        return response;
    }

    private BeedbException translate(String operation, String leaderAdd, Exception e){
        if(e instanceof BeedbException){
            return (BeedbException) e;
        }
        else if(e instanceof ConnectException || e instanceof SocketException){
            return new NodeUnreachableException("node " + leaderAdd + " unreachable" , e);
        }else if(e instanceof SocketTimeoutException){
            return new BeedbTimeoutException(operation + " failed due to timeout on "+leaderAdd , e);
        }
        return new BeedbException(operation+" failed on "+leaderAdd, e);
        
    }

    @PreDestroy 
    public void close(){
        pool.closeAllConnections();
    }
}
