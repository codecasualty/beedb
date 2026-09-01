package com.memcache.gateway.client;

import java.util.concurrent.ConcurrentHashMap;
import java.net.Socket;
import java.util.Map;
import com.memcache.gateway.client.BeedbConnection;
import java.util.Queue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.LinkedBlockingQueue;

public class ConnectionPool {
    
    ConcurrentHashMap<String , LinkedBlockingQueue<BeedbConnection>> connectionMap = new ConcurrentHashMap<>();
    private Logger LOGGER = LoggerFactory.getLogger(ConnectionPool.class.getName());

    public BeedbConnection getConnection(String key) throws Exception{
        BeedbConnection connection = connectionMap.computeIfAbsent(key, k -> new LinkedBlockingQueue<>()).poll();
        if(connection == null){
            connection = createConnection(key);
        }
        return connection;
    }

    public void putConnection(String key, BeedbConnection connection){
        if(connection == null)return;
        connectionMap.computeIfAbsent(key, k -> new LinkedBlockingQueue<>()).add(connection);
    }

    public BeedbConnection createConnection(String key) throws Exception{
        String[] address = key.split(":");
        int port = Integer.parseInt(address[1]);
        String ip = address[0];
        
        Socket socket = new Socket(ip, port);
        socket.setSoTimeout(7000);
        return new BeedbConnection(socket);
    }

    public void closeConnection(BeedbConnection connection){
        if(connection == null)return;
        connection.close();
    }

    public void closeAllConnections(){
        connectionMap.forEach((k,v) -> {
            try{
                v.forEach(s -> {
                    try {
                        s.close();
                    } catch (Exception e) {
                        LOGGER.error("something is wrong ",e);
                    }
                });
            }catch(Exception e){
                LOGGER.error("something is wrong ",e);
            }
        });
        connectionMap.clear();
    }
    
}
