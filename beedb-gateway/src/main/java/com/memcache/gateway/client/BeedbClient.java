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
import com.memcache.gateway.cluster.NodeStatus;

import jakarta.annotation.PreDestroy;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
public class BeedbClient {

    private final ConnectionPool pool;
    private final Map<String, String> nodes; // "node1" -> "127.0.0.1:11211"
    private volatile String leaderAddressString; // an address 
    private Logger LOGGER = LoggerFactory.getLogger(BeedbClient.class.getName());
    private final ExecutorService executorService;

    /**
     * How long a written key lives, in seconds.
     *
     * Fixed by the gateway rather than supplied by the caller: on a public demo a
     * visitor should not be able to ask for a ten-year TTL. Must stay under 30 days,
     * or memcached reads it as an absolute unix timestamp instead of a duration.
     */
    private final int expirySeconds;

    public BeedbClient(Map<String, String> nodes, int expirySeconds) {
        this.nodes = nodes;
        this.pool = new ConnectionPool();
        this.leaderAddressString = "";
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        if (expirySeconds < 0 || expirySeconds > 2592000) {
            throw new IllegalArgumentException(
                "expirySeconds must be between 0 and 2592000 (30 days), was " + expirySeconds);
        }
        this.expirySeconds = expirySeconds;
    }

    public String get(String key){
        return execute("get", connection -> connection.getValue(key));
    }

    public String set(String key, String value){
        return execute("set", connection -> connection.setValue(key, value, expirySeconds));
    }

    public String delete(String key){
        return execute("delete", connection -> connection.deleteValue(key));
    }


    /*
     * Nodes whose last stats request failed. The status poll asks every node once a second,
     * so a node that is down for a 15-second chaos round used to log the same WARN 15 times.
     * Logging only the two transitions -- gone, then back -- says the same thing.
     */
    private final java.util.Set<String> unreachableNodes = java.util.concurrent.ConcurrentHashMap.newKeySet();

    private void statsFailed(String operation, String address, Exception e){
        if(unreachableNodes.add(address)){
            logFailure(operation, address, e);
        }else{
            LOGGER.debug("{} still failing on node {} ({})", operation, address, e.toString());
        }
    }

    private void statsAnswered(String address){
        if(unreachableNodes.remove(address)){
            LOGGER.info("node {} is answering again", address);
        }
    }

    public NodeStatus getStats(String nodeId, String address){
        String response = null;
        BeedbConnection connection = null;
        try{
            connection = pool.getConnection(address);
            response = connection.getStats();
            pool.putConnection(address, connection);
            if(response == null) {
                return NodeStatus.unreachable(nodeId, address);
            }
            statsAnswered(address);
            return createNodeStatus(response , nodeId , address);
        }catch(Exception e){
            statsFailed("stats", address, e);
            pool.closeConnection(connection);
            return NodeStatus.unreachable(nodeId, address);
        }
    }

    public String getleaderAddressString() throws NoLeaderException{
        String leaderAdd = null;
        int retryCount = 0;
        NodeStatus nodeStatus = null;
        while(retryCount++ < 3){
            for(Map.Entry<String, String> entry : nodes.entrySet()){

                nodeStatus = getStats(entry.getKey() , entry.getValue());
                String role = NodeStatus.getRole(nodeStatus);
                if(role == null)continue;
                if("LEADER".equals(role)){
                    leaderAdd = entry.getKey();
                    break;
                }
            }
            if(leaderAdd != null)break;
            try{
                Thread.sleep(100);
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
        }else if(e instanceof NoLeaderException){
            // Expected for the second or two an election takes -- every chaos round causes one.
            LOGGER.warn("{} failed: no leader right now ({})", operation, e.getMessage());
        }else if(e instanceof java.util.concurrent.TimeoutException){
            // The status poll gives each node a fixed wait and records a slow one as unreachable
            // for that tick 
            LOGGER.warn("{}: node {} did not answer in time", operation, address);
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

    public List<NodeStatus> getClusterStatus(){
        List<NodeStatus> nodeStatuses = new ArrayList<>();
        List<CompletableFuture<NodeStatus>> futures = new ArrayList<>();
        List<Map.Entry<String,String>> entries = new ArrayList<>(nodes.entrySet());
        for(Map.Entry<String , String> entry : entries){
            CompletableFuture<NodeStatus> future = CompletableFuture.supplyAsync(() -> getStats(entry.getKey() , entry.getValue()),executorService);
            futures.add(future);
        }
        for(int i = 0 ; i < futures.size() ; i++){
            CompletableFuture<NodeStatus> future = futures.get(i);
            try{
                NodeStatus result = future.get(1000, TimeUnit.MILLISECONDS);
                nodeStatuses.add(result);
            }catch(Exception e){
                statsFailed("stats poll", entries.get(i).getValue(), e);
                nodeStatuses.add(NodeStatus.unreachable(entries.get(i).getKey() , entries.get(i).getValue()));
            }
        }
        return nodeStatuses;
    }

    public NodeStatus createNodeStatus(String response , String nodeId , String address){
        String[] parts = response.split("\n");
        Map<String , String> map = new HashMap<>();
        for(String part : parts){
            if(part.equals("END")) break;
            if(!part.startsWith("STAT")) return NodeStatus.unreachable(nodeId, address);
            String[] keyValue = part.split(" ");
            map.put(keyValue[1] , keyValue[2]);
        }
        return NodeStatus.of(
            getIntValue("curr_items" , map), nodeId, getValue("raft_role" , map), getLongValue("raft_term" , map), getIntValue("raft_commit_index" , map),
            getIntValue("raft_last_applied" , map), map.get("raft_leader_id"), getIntValue("raft_last_included_index" , map), getIntValue("raft_log_size" , map)
            );
    }

    public Integer getIntValue(String key , Map<String , String> nodes){
        if(nodes.containsKey(key)) return Integer.parseInt(nodes.get(key));
        else return null;
    }

    public Long getLongValue(String key , Map<String , String> nodes){
        if(nodes.containsKey(key)) return Long.parseLong(nodes.get(key));
        else return null;
    }

    public String getValue(String key , Map<String , String> nodes){
        if(nodes.containsKey(key)) return nodes.get(key);
        else return null;
    }

    public String getLeaderAddress(){
        return leaderAddressString;
    }

    @PreDestroy 
    public void close(){
        pool.closeAllConnections();
        executorService.shutdown();
    }
}
