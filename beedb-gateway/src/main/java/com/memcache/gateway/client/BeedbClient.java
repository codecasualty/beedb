/*
 * Copyright 2026 subodh.
 * this class opens a socket connection to local host : 11211 port where beedb-server is running
 * and then sends commands to beedb-server
 */
package com.memcache.gateway.client;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        if(leaderAddressString == null || leaderAddressString.isEmpty()){
            leaderAddressString = getleaderAddressString();
        }
        String response = null;
        BeedbConnection connection = null;
        try{
            connection = pool.getConnection(leaderAddressString);
            response = connection.getValue(key);
            pool.putConnection(leaderAddressString, connection);
        }catch(Exception e){
            leaderAddressString = null;
            LOGGER.error("something is wrong ",e);
            pool.closeConnection(connection);
        }
        
        return response;
    }

    public String set(String key, String value){
        if(leaderAddressString == null || leaderAddressString.isEmpty()){
            leaderAddressString = getleaderAddressString();
        }
        BeedbConnection connection = null;
        String response = null;
        try{
            connection = pool.getConnection(leaderAddressString);
            response = connection.setValue(key, value);
            if(response != null && response.startsWith("SERVER_ERROR")){
                pool.putConnection(leaderAddressString, connection);
                leaderAddressString = getleaderAddressString();
                connection = pool.getConnection(leaderAddressString);
                response = connection.setValue(key, value);
            }
            pool.putConnection(leaderAddressString, connection);
        
        }catch(Exception e){
            leaderAddressString = null;
            LOGGER.error("something is wrong ",e);
            pool.closeConnection(connection);
        }
        
        return response;
    }

    public String delete(String key){
        if(leaderAddressString == null || leaderAddressString.isEmpty()){
            leaderAddressString = getleaderAddressString();
        }
        BeedbConnection connection = null;
        String response = null;
        try{
            connection = pool.getConnection(leaderAddressString);
            response = connection.deleteValue(key);
            if(response != null && response.startsWith("SERVER_ERROR")){
                pool.putConnection(leaderAddressString, connection);
                leaderAddressString = getleaderAddressString();
                connection = pool.getConnection(leaderAddressString);
                response = connection.deleteValue(key);
            }
            pool.putConnection(leaderAddressString, connection);
            
        }catch(Exception e){
            leaderAddressString = null;
            LOGGER.error("something is wrong ",e);
            pool.closeConnection(connection);
        }
        
        return response;
    }


    public String getStats(String address){
        String response = null;
        BeedbConnection connection = null;
        try{
            connection = pool.getConnection(address);
            response = connection.getStats();
            pool.putConnection(address, connection);
        }catch(Exception e){
            LOGGER.error("something is wrong ",e);
            pool.closeConnection(connection);
        }
        return response;
    }

    public String getleaderAddressString(){
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
                LOGGER.error("something is wrong ",e);
            }   
        }
        if(leaderAdd != null) leaderAddressString = nodes.get(leaderAdd);
        return leaderAdd != null ? nodes.get(leaderAdd) : "NO LEADER";
    }

    

}
