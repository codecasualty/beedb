/*
* Main Server class for memcache server
* Author : Subodh
* Date   : 2022-01-10
* mail   : trycodeforfun@gmail.com
 */

package com.memcache;
import java.io.ByteArrayOutputStream;
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
import java.nio.charset.StandardCharsets;
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
    private static final int INITIAL_BUFFER_SIZE = 4 * 1024;
    private static final int MAX_BUFFER_SIZE = 8 * 1024;
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
                    key.attach(ByteBuffer.allocate(INITIAL_BUFFER_SIZE));
                    accept(selector, serverSocketChannel);
                }
                else if(key.isReadable()){
                    read(selector, key, executorService);
                }
                else if(key.isWritable()){
                    write(key, selector, executorService);
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
        ByteBuffer channelBuffer = (ByteBuffer) key.attachment();
        if(channelBuffer == null){
            channelBuffer = ByteBuffer.allocate(INITIAL_BUFFER_SIZE);
            key.attach(channelBuffer);
        }

        int bytesRead = socketChannel.read(channelBuffer);
        LOGGER.debug("read {} bytes; buffer holds {} of {}",
                bytesRead, channelBuffer.position(), channelBuffer.capacity());

        // -1 is the ONLY case that means the peer is gone. Closing the channel
        // cancels its key too, so cancel() on its own would leave the socket open
        // and merely make the selector deaf to it -- a leaked fd in CLOSE_WAIT.
        if(bytesRead == -1){
            socketChannel.close();
            return;
        }

        if(bytesRead == 0){
            if(channelBuffer.hasRemaining()){
                // Nothing new arrived and there is room for more. Returning is safe
                // because select() will not report this key again until data lands.
                return;
            }
            // No room left, so no future read can EVER make progress -- returning
            // here is an infinite spin, because the key stays readable forever.
            // Either grow, or refuse.
            if(channelBuffer.capacity() >= MAX_BUFFER_SIZE){
                LOGGER.warn("command exceeds {} bytes from {}; refusing",
                        MAX_BUFFER_SIZE, socketChannel.getRemoteAddress());
                sendResponse(key, "SERVER_ERROR object too large for cache\r\n".getBytes(), selector);
                // The stream is now at an unknown offset -- we cannot tell where the
                // next command starts -- so the connection cannot be resynchronised.
                // Closing is the honest option. (memcached can stay open because it
                // swallows exactly <bytes> and lands on a known boundary; that needs
                // a discard-N-bytes mode we do not have.)
                key.cancel();
                return;
            }
            ByteBuffer grown = ByteBuffer.allocate(MAX_BUFFER_SIZE);
            channelBuffer.flip();          // without this, put() copies nothing
            grown.put(channelBuffer);
            channelBuffer = grown;
            key.attach(channelBuffer);     // the buffer lives on the KEY, not in a local
            LOGGER.debug("grew buffer to {} bytes", MAX_BUFFER_SIZE);
        }

        // Dispatch at most ONE command. Looping here would hand several commands to
        // the executor at once, and their responses would race onto the same socket --
        // memcached clients match replies to requests BY ORDER, so that corrupts the
        // session. The next command (if the buffer already holds one) is picked up in
        // write(), once this response has gone out.
        tryDispatchOneCommand(key, channelBuffer, selector, executorService);
    }


    public boolean tryDispatchOneCommand(SelectionKey key, ByteBuffer channelBuffer, Selector selector, ExecutorService executorService) throws IOException{
        int i = 0;
        boolean endFound = false;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int lastIndex = channelBuffer.position();
        while(i < lastIndex){
            char c = (char) channelBuffer.get(i++);
            if(c == '\n'){
                endFound = true;
                break;
            }
            baos.write(c);
        }
        LOGGER.info("end found is {}", endFound);
        LOGGER.info("command from client: {}", baos.toString());
        if(!endFound){
            LOGGER.info(" position is {} and limit is {} capacity is {} ", 
            channelBuffer.position(), channelBuffer.limit(), channelBuffer.capacity());
            if(channelBuffer.position() == channelBuffer.capacity()){
                if(channelBuffer.capacity() == MAX_BUFFER_SIZE){
                    sendResponse(key, ("Max Value length exceeded\r\n").getBytes(), selector);
                    // i dont think we should close the socket here , becaue client may try to send by correcting its mistake 
                    key.cancel();
                    // socketChannel.close();
                    return false;
                }else{
                    ByteBuffer newBuffer = ByteBuffer.allocate(MAX_BUFFER_SIZE);
                    newBuffer.put(channelBuffer);
                    channelBuffer = newBuffer;
                    key.attach(newBuffer);
                    return false;
                }
            }
            else
                return false;
        }
        byte[] buffer = baos.toByteArray();
        
        String commandLine = null;
        if(buffer.length > 0 && buffer[buffer.length-1] == '\r')
            commandLine = new String(buffer, 0, buffer.length-1, StandardCharsets.UTF_8);
        else return false;
        
        // this will eliminate the \r\n
        LOGGER.info("command from client: {}", commandLine);
        Command command = null;
        try{
            command = CommandParser.parse(commandLine);
            int valueLength = command.getByteLength();
            LOGGER.info("value length is {} ", valueLength);
            LOGGER.info("last index is {} ", lastIndex);
            LOGGER.info("index i is at {} ", i);
        
            // for get/ stats/ delete there is no value length
            if(valueLength == -1) {
                channelBuffer.flip();
                channelBuffer.position(i);
                channelBuffer.compact();
            }
        
            // for other commands we will have >= 0 value length
        
        
            else if(valueLength + i + 2 > lastIndex){
                return false;
            }
            else if(valueLength + i + 2 <= lastIndex){
                byte[] valueBytes = new byte[valueLength];
                for(int j = 0; j < valueLength; j++){
                    valueBytes[j] = (byte) channelBuffer.get(i++);
                }
                
                command.setValue(valueBytes);
                // compact considers everythign between position to limit as unread bytes
                channelBuffer.flip();
                // because i has already moved valuedLength positoins ahead
                channelBuffer.position(i + 2);
                channelBuffer.compact();
            }
            LOGGER.info("final position is {} limit is {} capacity is {} ", channelBuffer.position(), channelBuffer.limit(), channelBuffer.capacity());
        }catch(Exception e){
            LOGGER.error("Error while parsing command : {} ", e.getMessage());
            sendResponse(key, ("ERROR\r\n").getBytes(), selector);
        }
        
        
        // we have to read 4096 bytes to get the command and then ask command parse to give us length of bytes to read next
        // then we have to read those many bytes and thats our complete command
        // for example:-
        // set foo 0 300 5\r\n
        // hello
        // that means first we read complete first line and then pass it to command parser to give us length of value bytes to read and then read
        // those many bytes and thats our value
        // we can use ByteBuffer to read bytes from socketChannel
        
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        final Command cmd = command;
        executorService.execute(() -> {
            try{
                byte[] response = processRequest(cmd);
                LOGGER.info("response which is sent is {}", new String(response));
                sendResponse(key, response, selector);
            }catch (Exception e){
                LOGGER.error("something is wrong {}",e);
                key.cancel();
            }
        });

        return true;
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
                LOGGER.info("command type is {}", command.getType());
                LOGGER.info("command is {}", command);
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

    /**
     * Flush one queued response, then look for the NEXT command already in the buffer.
     *
     * That second half is the pipelining fix. If a client sends two commands in one
     * write, the second is sitting in this connection's buffer once the first is
     * consumed -- and NO further readable event is coming, because the client has sent
     * everything and is blocked waiting for replies. Nothing would ever look at those
     * bytes again, and the client would hang until its socket timeout.
     *
     * Doing it here rather than looping inside read() keeps exactly one command in
     * flight per connection, so responses go out in request order. memcached clients
     * match replies to requests by position, not by amany id, so ordering is not
     * cosmetic -- interleaved responses hand the client the wrong answers.
     */
    public void write(SelectionKey key, Selector selector, ExecutorService executorService) throws IOException {
        
        Map.Entry<SelectionKey, ByteBuffer> entry = pendingWrites.poll();
        if(entry == null) return;
        SelectionKey  selectionKey = entry.getKey();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer responseBuffer = entry.getValue();
        LOGGER.debug("writing response to client {} ", new String(responseBuffer.array(), 0, responseBuffer.limit()));
        while(responseBuffer.hasRemaining()){
            socketChannel.write(responseBuffer);
        }
        // Re-arm reads: this connection is free to carry another command.
        selectionKey.interestOps(SelectionKey.OP_READ);

        // The buffer may ALREADY hold a complete command from the same TCP segment.
        // Nothing will wake us for it, so try now.
        ByteBuffer channelBuffer = (ByteBuffer) selectionKey.attachment();
        if(channelBuffer != null && selectionKey.isValid()){
            tryDispatchOneCommand(selectionKey, channelBuffer, selector, executorService);
        }
    }

    public void close(){
        if(raftRpcServer != null) raftRpcServer.close();
        if(raftNode != null) raftNode.stop();    }
    
}
