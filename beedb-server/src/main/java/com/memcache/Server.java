/*
* Main Server class for memcache server
* Author : Subodh
* Date   : 2022-01-10
* mail   : trycodeforfun@gmail.com
 */

package com.memcache;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
    // memcached: an expiry above 30 days is an absolute Unix time, not seconds-from-now.
    private static final long MAX_RELATIVE_EXPIRY_SECONDS = 30L * 24 * 60 * 60;
    public static void main(String[] args) throws IOException{
        // read file name
        if(args.length == 0){
            configError("usage: java -jar beedb-server.jar <node>.properties");
        }
        String fileName = args[0];
        // read properties file
        Properties properties = new Properties();
        try(
            FileInputStream fileInputStream = new FileInputStream(fileName);
            Server server = new Server();
        ){
            properties.load(fileInputStream);

            ArrayList<String> peers = parsePeers(properties.getProperty("peers"));
            // if properties values are absent then we use default values

            Runtime.getRuntime().addShutdownHook(new Thread(server::close));
            String nodeId = properties.getProperty("nodeId", "node1");
            int clientPort = requiredPort(properties, "clientPort");
            int raftPort = requiredPort(properties, "raftPort");
            if(clientPort == raftPort){
                throw new IllegalArgumentException("clientPort and raftPort are both " + clientPort
                    + "; a node cannot serve clients and raft on one port");
            }
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
            server.start(peers, nodeId , clientPort , raftPort, stateDir, snapshotDir, tmpDir, walDir, snapShotLimit, snapShotThreshold, minElectionTimeout, maxElectionTimeout, heartbeatInterval, peerRetryBackoffInitialMs, peerRetryBackoffMaxMs);
        }catch(IllegalArgumentException e){
            // every config problem lands here: the node never starts, so it can
            // never sit in the cluster listening but unable to take part.
            configError(e.getMessage());
        }catch(FileNotFoundException e){
            configError("config file not found: " + fileName);
        }catch(Exception e){
            LOGGER.error("something is wrong {}",e);
            System.exit(1);
        }
        
    }
    
    /**
     * peers is "host:port,host:port"
     */
    private static ArrayList<String> parsePeers(String raw) {
        if (raw == null || raw.isBlank()) {
            throw new IllegalArgumentException(
                "peers is required, e.g. peers=127.0.0.1:12222,127.0.0.1:12223");
        }
        ArrayList<String> peers = new ArrayList<>();
        for (String entry : raw.split(",")) {
            String peer = entry.trim();
            if (peer.isEmpty()) {
                throw new IllegalArgumentException("peers has an empty entry: '" + raw + "'");
            }
            // lastIndexOf, not indexOf: an IPv6 literal has colons of its own
            int colon = peer.lastIndexOf(':');
            if (colon <= 0 || colon == peer.length() - 1) {
                throw new IllegalArgumentException("peer '" + peer + "' must be host:port");
            }
            parsePort(peer.substring(colon + 1), "port of peer '" + peer + "'");
            if (peers.contains(peer)) {
                throw new IllegalArgumentException("peers lists '" + peer + "' twice");
            }
            peers.add(peer);
        }
        return peers;
    }

    /** A port this node binds. Must be present, numeric and a real port number. */
    private static int requiredPort(Properties properties, String key) {
        String raw = properties.getProperty(key);
        if (raw == null) {
            throw new IllegalArgumentException("property '" + key + "' is required");
        }
        return parsePort(raw, key);
    }

    private static int parsePort(String raw, String what) {
        int value;
        try {
            value = Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(what + " is not a number: '" + raw.trim() + "'");
        }
        if (value < 1 || value > 65535) {
            throw new IllegalArgumentException(what + " is out of range: " + value);
        }
        return value;
    }

    private static void configError(String message) {
        LOGGER.error("bad configuration: {}", message);
        System.err.println("bad configuration: " + message);
        System.exit(2);
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

    /**
     * the client's memcached expiry, as an absolute deadline in epoch milliseconds.
     *   0              never expires              -> 0
     *   1 .. 30 days   seconds from now           -> now + seconds
     *   above 30 days  an absolute Unix time      -> that time, in milliseconds
     *   negative       already expired            -> a moment in the past
     */
    static long absoluteExpiryMillis(long expiry, long nowMillis){
        if(expiry == 0) return 0;
        if(expiry > MAX_RELATIVE_EXPIRY_SECONDS) return expiry * 1000L;
        return nowMillis + expiry * 1000L;
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
                try{

                    if(key.isAcceptable()){
                        // No buffer here: this is the LISTENING socket's key, which never
                        // reads. Each accepted connection gets its own buffer in read().
                        accept(selector, serverSocketChannel);
                    }
                    else if(key.isReadable()){
                        read(selector, key, executorService);
                    }
                    else if(key.isWritable()){
                        write(key, selector, executorService);
                    }
                }catch(Exception e){
                    LOGGER.error("something is wrong {}",e);
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
        // and merely make the selector deaf to it that means we have leaked fd in CLOSE_WAIT.
        if(bytesRead == -1){
            socketChannel.close();
            return;
        }

        if(bytesRead == 0){
            if(channelBuffer.hasRemaining()){
                // Nothing new arrived and there is room for more reeturning is safe
                // because select() will not report this key again until data lands.
                return;
            }
            // no room left, so no future read can EVER make progress
            // Either grow, or refuse.
            if(channelBuffer.capacity() >= MAX_BUFFER_SIZE){
                LOGGER.warn("command exceeds {} bytes from {}; refusing",
                        MAX_BUFFER_SIZE, socketChannel.getRemoteAddress());
                sendResponse(key, "SERVER_ERROR object too large for cache\r\n".getBytes(), selector);
                // the stream is now at an unknown offset , we cannot tell where the
                // next command starts , so the connection cannot be resynchronised.
                key.cancel();
                return;
            }
            ByteBuffer grown = ByteBuffer.allocate(MAX_BUFFER_SIZE);
            channelBuffer.flip();          // without this, put() copies nothing
            grown.put(channelBuffer);
            channelBuffer = grown;
            key.attach(channelBuffer);     
            LOGGER.debug("grew buffer to {} bytes", MAX_BUFFER_SIZE);
        }

        // dispatch at most ONE command looping here would hand several commands to
        // the executor at once, and their responses would race onto the same socket
        // memcached clients match replies to requests BY ORDER, so that corrupts the
        // session. The next command (if the buffer already holds one) is picked up in
        // write(), once this response has gone out.
        tryDispatchOneCommand(key, channelBuffer, selector, executorService);
    }


    /**
     * Take at most ONE complete command out of the connection's buffer and dispatch it.
     *
     * the contract, which every early return depends on: 
     * if false : then nothing is consumer , leave buffer as it was found 
     * if true : then we have consumed something.
     * the buffer IS the state; nothing is remembered between calls, so a half-finished parse cannot leave anything
     * inconsistent.
     *
     * the condition is the one that bites: any path that gives up must not have moved
     * position, and any path that HANDLES something must consume it. A path that
     * neither consumes nor makes progress is an infinite spin, because the selector is
     * level-triggered and will hand us the same bytes forever.
     */
    public boolean tryDispatchOneCommand(SelectionKey key, ByteBuffer channelBuffer, Selector selector, ExecutorService executorService) throws IOException{
        // write mode: position IS the number of bytes held.
        final int held = channelBuffer.position();

        // scan with the ABSOLUTE get(int) so position stays untouched 
        // we do not consume anything until we know the whole command is present.
        int lineEnd = -1;
        for(int idx = 0; idx < held; idx++){
            if(channelBuffer.get(idx) == '\n'){ lineEnd = idx; break; }
        }

        if(lineEnd < 0){
            // no complete line yet.
            if(channelBuffer.position() < channelBuffer.capacity()){
                return false;                       // room left; more bytes can arrive
            }
            // buffer is FULL with no line in it, so no future read can make progress.
            if(channelBuffer.capacity() >= MAX_BUFFER_SIZE){
                LOGGER.warn("command line exceeds {} bytes; closing connection", MAX_BUFFER_SIZE);
                sendResponse(key, "SERVER_ERROR object too large for cache\r\n".getBytes(), selector);
                key.channel().close();
                return false;
            }
            ByteBuffer grown = ByteBuffer.allocate(MAX_BUFFER_SIZE);
            channelBuffer.flip();                   // without this put() copies nothing
            grown.put(channelBuffer);
            key.attach(grown);                      // the buffer belongs to the KEY
            return false;
        }

        // the line is bytes [0, lineEnd); strip a trailing '\r' if present doing it by
        // index avoids the lookbehind arithmetic 
        int lineLength = lineEnd;
        if(lineLength > 0 && channelBuffer.get(lineLength - 1) == '\r') lineLength--;
        byte[] lineBytes = new byte[lineLength];
        for(int idx = 0; idx < lineLength; idx++) lineBytes[idx] = channelBuffer.get(idx);
        String commandLine = new String(lineBytes, StandardCharsets.UTF_8);

        final int afterLine = lineEnd + 1;          // first byte past the '\n'

        Command command;
        try{
            command = CommandParser.parse(commandLine);
        }catch(Exception e){
            // consume the bad line before replying returning without consuming would
            // re-parse the same garbage on every wakeup 
            consume(channelBuffer, held, afterLine);
            LOGGER.debug("unparseable command line: {}", commandLine);
            sendResponse(key, "ERROR\r\n".getBytes(), selector);
            return true;                            // we made progress
        }

        final int valueLength = command.getByteLength();
        final int consumedEnd;

        if(valueLength < 0){
            // get / delete / stats -- no data block, so the line IS the whole command.
            consumedEnd = afterLine;
        }else{
            // data block is valueLength bytes plus its own trailing CRLF ,note 0 is a
            // legal length (an empty value still has the CRLF), which is why -1 rather
            // than 0 marks "no data block at all".
            int need = valueLength + 2;
            if(held - afterLine < need) return false;   // value not all here yet
            byte[] valueBytes = new byte[valueLength];
            for(int j = 0; j < valueLength; j++) valueBytes[j] = channelBuffer.get(afterLine + j);
            command.setValue(valueBytes);
            consumedEnd = afterLine + need;
        }

        consume(channelBuffer, held, consumedEnd);

        // one command in flight per connection: stop reading until the response is out.
        // without this, a second command could be dispatched concurrently and the two
        // responses could interleave
        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        final Command cmd = command;
        executorService.execute(() -> {
            try{
                sendResponse(key, processRequest(cmd), selector);
            }catch (Exception e){
                LOGGER.error("failed to process {}", cmd.getType(), e);
                try{ key.channel().close(); }catch(Exception ignored){ }
            }
        });
        return true;
    }

    /**
     * lets say we receive command like command1|command2|command3|command4|command5
     * then held will be last index of command5 that means we have that many bytes in our buffer (held here means count) (here limit = capacity = 4k)
     * and now lets say we have processed till command2 so we have consumed command1|command2 lets say that 
     * index is 20 ,so our position is at index of command2 , so we set position to held index i.e. lets say index 50 
     * so when we flip that means we have moved from write to read mode that means position = 0 and limit (50)
     * flip (limit  = position and poisiton = 0) i.e. position = 0 and limit = 50 then we set position to upTo index i.e. 
     * how many we have consumed , so that means
     * position = 20 , now when we compact that means everything between position to limit is yet to be consumed i.e. 20..50
     */
    private void consume(ByteBuffer buffer, int held, int upTo){
        buffer.position(held);      // make sure flip() sees the real byte count
        buffer.flip();              // limit = held, position = 0
        buffer.position(upTo);      // skip what we used
        buffer.compact();           // keeps [upTo, held), back to write mode
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
                command.setExpiry(absoluteExpiryMillis(command.getExpiry(), System.currentTimeMillis())); 
                LOGGER.debug("command type is {}", command.getType());
                LOGGER.debug("command is {}", command);
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
        if(selectionKey.isValid() == false) return;
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
