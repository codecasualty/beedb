/*
* Main Server class for memcache server
* Author : Subodh
* Date   : 2022-01-10
* mail   : trycodeforfun@gmail.com
 */

package com.memcache;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.command.CommandParser;
import com.memcache.command.CommandType;
import com.memcache.handler.CommandProcessor;
import com.memcache.response.Response;
import com.memcache.raft.SocketRaftTransport;
import com.memcache.raft.RaftTransport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.raft.RaftNode;
import com.memcache.raft.RaftRpcServer;

import java.util.Iterator;
import java.util.Set;
import java.nio.channels.SocketChannel;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.ArrayList;
public class Server implements AutoCloseable{

    ConcurrentLinkedQueue<Map.Entry<SelectionKey, ByteBuffer>> pendingWrites = new ConcurrentLinkedQueue<>();
    RaftNode  raftNode;
    Cache  cache ;
    RaftRpcServer raftRpcServer;
    private static Logger LOGGER = LoggerFactory.getLogger(Server.class.getName());
    private RaftTransport raftTransport;

    public static void main(String[] args) throws IOException{
        // read file name
        String fileName = args[0];
        // read properties file
        Properties properties = new Properties();
        try(
            FileInputStream fileInputStream = new FileInputStream(fileName);
            Server server = new Server();
        ){
            properties.load(fileInputStream);
            
            ArrayList<String> peers = new ArrayList<>();
            for(String peer : properties.getProperty("peers").split(",")){
                peers.add(peer);
            }
            // if properties values are absent then we use default values

            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            String nodeId = properties.getProperty("nodeId", "node1");
            int clientPort = Integer.parseInt(properties.getProperty("clientPort"));
            int raftPort = Integer.parseInt(properties.getProperty("raftPort"));
            String stateDir = properties.getProperty("stateDir" , "state");
            String snapshotDir = properties.getProperty("snapshotDir" , "snapshot");
            String tmpDir = properties.getProperty("tmpDir" , "tmp");
            String walDir = properties.getProperty("walDir", "wal");
            int snapShotLimit = intProperty(properties, "snapShotLimit", 1000);
            int snapShotThreshold = intProperty(properties, "snapShotThreshold", 1000);
            int minElectionTimeout = intProperty(properties, "electionTimeoutMinMs", 1000);
            int maxElectionTimeout = intProperty(properties, "electionTimeoutMaxMs", 2000);
            int heartbeatInterval = intProperty(properties, "heartbeatIntervalMs", 300);
            int peerRetryBackoffInitialMs = intProperty(properties, "peerRetryBackoffInitialMs", 200);
            int peerRetryBackoffMaxMs = intProperty(properties, "peerRetryBackoffMaxMs", 1000);
            LOGGER.info("timing budget: election {}-{}ms, heartbeat {}ms, ratio {}",
                minElectionTimeout, maxElectionTimeout, heartbeatInterval,
                String.format("%.1f", minElectionTimeout / (double) heartbeatInterval));
            LOGGER.debug("clientPort is {} and raftPort is {} and peers is {} ", clientPort , raftPort, peers);
            LOGGER.debug("stateDir is {} and snapshotDir is {} and tmpDir is {} and walDir is {} ", stateDir , snapshotDir, tmpDir, walDir);
            LOGGER.debug("snapShotLimit is {} and snapShotThreshold is {} and minElectionTimeout is {} and maxElectionTimeout is {} and heartbeatInterval is {} ", snapShotLimit, snapShotThreshold, minElectionTimeout, maxElectionTimeout, heartbeatInterval);
            if(clientPort == 0 || raftPort == 0 || peers.size() == 0){
                throw new IllegalArgumentException("clientPort and raftPort must be provided");
            }
            server.start(peers, nodeId , clientPort , raftPort, stateDir, snapshotDir, tmpDir, walDir, snapShotLimit, snapShotThreshold, minElectionTimeout, maxElectionTimeout, heartbeatInterval, peerRetryBackoffInitialMs, peerRetryBackoffMaxMs);
        }catch(Exception e){
            LOGGER.error("something is wrong {}",e);
            System.exit(1);
        }
        
    }
    
    private static int intProperty(Properties properties, String key, int defaultValue) {
        String raw = properties.getProperty(key);
        if (raw == null) {
            LOGGER.warn("property '{}' not set, defaulting to {} -- is the key spelled correctly? :(",
                        key, defaultValue);
            return defaultValue;
        }
        return Integer.parseInt(raw.trim());
    }

    public void start(ArrayList<String> peers, String nodeId, int clientPort,int raftPort, String stateDir, String snapshotDir, 
        String tmpDir, String walDir, int snapShotLimit, int snapShotThreshold, int minElectionTimeout, int maxElectionTimeout, int heartbeatInterval , int peerRetryBackoffInitialMs, int peerRetryBackoffMaxMs) throws IOException, InterruptedException{
        // selector to notify about new connections
        if(peers.size() == 0) throw new IllegalArgumentException("No peers provided");
        if(nodeId == null) throw new IllegalArgumentException("No nodeId provided");
        cache = new Cache();
        raftTransport = new SocketRaftTransport(peers);
        raftNode = new RaftNode(peers, nodeId, cache , raftTransport, stateDir, snapshotDir, tmpDir ,walDir, snapShotLimit, snapShotThreshold, minElectionTimeout, maxElectionTimeout, heartbeatInterval , peerRetryBackoffInitialMs, peerRetryBackoffMaxMs);   
        raftRpcServer = new RaftRpcServer(raftPort, raftNode);
        raftNode.start();
        raftRpcServer.start();
        Selector selector = Selector.open();
        // server socket to listen for new connections
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(clientPort));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector , SelectionKey.OP_ACCEPT);
    
        // we will use virtual threads instead of fixed thread pool 
        ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();
        // we keep on listening for connections and accept those connection
        // and create threads to work on those connections
        while(true){
            int numKeys = selector.select();
            if(numKeys == 0) continue;
    
            Set<SelectionKey> keys = selector.selectedKeys();
            Iterator<SelectionKey> iterator = keys.iterator();
    
            while(iterator.hasNext()){
                SelectionKey key = iterator.next();
                iterator.remove();
    
                if(key.isAcceptable()){
                    accept(selector, serverSocketChannel);
                }
                else if(key.isReadable()){
                    read(selector, key, executorService);
                }
                else if(key.isWritable()){
                    write(key);
                }
            }
        }
        
    }

    public void accept(Selector selector, ServerSocketChannel serverSocketChannel) throws IOException{
        SocketChannel SocketChannel = (SocketChannel) serverSocketChannel.accept();
        Socket socket = SocketChannel.socket();
        socket.setSoTimeout(1000);
        SocketChannel.configureBlocking(false);
        SocketChannel.register(selector, SelectionKey.OP_READ);
        LOGGER.debug("Accepted connection {} ", socket.getInetAddress());
    }



    public void read(Selector selector, SelectionKey key, ExecutorService executorService) throws IOException{
        SocketChannel socketChannel = (SocketChannel) key.channel();

        // we have to read 4096 bytes to get the command and then ask command parse to give us length of bytes to read next
        // then we have to read those many bytes and thats our complete command
        // for example:-
        // set foo 0 300 5\r\n
        // hello
        // that means first we read complete first line and then pass it to command parser to give us length of value bytes to read and then read
        // those many bytes and thats our value
        // we can use ByteBuffer to read bytes from socketChannel

        ByteBuffer buffer = ByteBuffer.allocate(4096);
        int read = socketChannel.read(buffer);
        if(read == -1){
            key.cancel();
            socketChannel.close();
            return;
        }
        buffer.flip();
        StringBuilder commandLine = new StringBuilder();
        while(buffer.hasRemaining()){
            char c = (char) buffer.get();
            if(c == '\r'){
                break;
            }
            LOGGER.debug("reading character : {} ", c);
            commandLine.append(c);
        }
        // below code ensure that we read \n after \r and \n is not part of command
        if(buffer.hasRemaining()) buffer.get();
        Command command = null;
        byte[] valueBytes = null;
        LOGGER.debug("command from client: {}", commandLine.toString());
        try{
            command = CommandParser.parse(commandLine.toString());
            LOGGER.debug("command from client: {}", command);
            int valueLength = command.getByteLength();
            if(valueLength >= 0){
                valueBytes = new byte[valueLength];
                for(int i = 0; i < valueLength; i++){
                    valueBytes[i] = (byte) buffer.get();
                }
                command.setValue(valueBytes);
            }
        }
        catch (Exception e){
            LOGGER.debug("Error while parsing command : {} ", e.getMessage());
            sendResponse(key, ("ERROR\r\n").getBytes(), selector);
            return;
        }

        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        final Command cmd = command;
        executorService.execute(() -> {
            try{
                byte[] response = processRequest(cmd);
                sendResponse(key, response, selector);
            }catch (Exception e){
                // TODO: handle exception
                LOGGER.error("something is wrong {}",e);
            }
        });

        
    }

    public void sendResponse(SelectionKey key, byte[] response, Selector selector) throws IOException{
        ByteBuffer responseBuffer = ByteBuffer.wrap(response);
        pendingWrites.add(Map.entry(key, responseBuffer));
        key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
        selector.wakeup();
    }

    public byte[] processRequest(Command command) throws Exception{
        
        try {
            if(command.getType() == CommandType.STATS){
                Map<String , String> stats = raftNode.getStats();
                StringBuilder builder = new StringBuilder();
                for(String key : stats.keySet()){
                    builder.append("STAT ");
                    builder.append(key);
                    builder.append(" ");
                    builder.append(stats.get(key));
                    builder.append("\r\n");
                }
                builder.append("END\r\n");
                LOGGER.debug("build string is {} ",builder.toString());
                return builder.toString().getBytes();
            }
            else if(command.getType() == CommandType.GET){
                Response response = CommandProcessor.process(command, cache);
                return response.toProtocolString().getBytes();
            }
            else{
                // we have to propose this command to raft node
                // and get the output future from propose method
                Future<String> future = raftNode.propose(command.serialize());
                // we have to wait for the response from raft node
                // .get() blocks until response is available so we have used timeout
                String response = future.get(5 , TimeUnit.SECONDS);
                LOGGER.debug("response from raft node : {}", response);
                return response.getBytes();
            }                
        } catch (Exception e) {
            // TODO: handle exception
            LOGGER.error("something is wrong {}",e);
            if(e instanceof ExecutionException && e.getCause() instanceof IllegalStateException){
                String message  = "SERVER_ERROR " + ((IllegalStateException) e.getCause()).getMessage()+"\r\n";
                return message.getBytes();
            }
            else if(e instanceof TimeoutException){
                return "SERVER_ERROR timeout\r\n".getBytes();
            }
            return "SERVER_ERROR\r\n".getBytes();
            
        }
    }

    public void write(SelectionKey key) throws IOException {
        
        Map.Entry<SelectionKey, ByteBuffer> entry = pendingWrites.poll();
        if(entry == null) return;
        SelectionKey  selectionKey = entry.getKey();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer responseBuffer = entry.getValue();
        LOGGER.debug("writing response to client {} ", new String(responseBuffer.array(), 0, responseBuffer.limit()));
        while(responseBuffer.hasRemaining()){
            socketChannel.write(responseBuffer);
        }// making selector ready for read operation
        selectionKey.interestOps(SelectionKey.OP_READ);
    }

    public void close(){
        if(raftRpcServer != null) raftRpcServer.close();
        if(raftNode != null) raftNode.stop();    }
    
}
