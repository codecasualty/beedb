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
import java.util.Arrays;
import java.util.ArrayList;
public class Server {

    ConcurrentLinkedQueue<Map.Entry<SelectionKey, ByteBuffer>> pendingWrites = new ConcurrentLinkedQueue<>();
    RaftNode  raftNode;
    Cache  cache ;
    RaftRpcServer raftRpcServer;
    private Logger LOGGER = LoggerFactory.getLogger(Server.class.getName());
    private RaftTransport raftTransport;

    public static void main(String[] args) throws IOException{
        // read file name
        String fileName = args[0];
        // read properties file
        Properties properties = new Properties();
        try(
            FileInputStream fileInputStream = new FileInputStream(fileName);
        ){
            properties.load(fileInputStream);
            
            ArrayList<String> peers = new ArrayList<>();
            for(String peer : properties.getProperty("peers").split(",")){
                peers.add(peer);
            }
            String nodeId = properties.getProperty("nodeId");
            int clientPort = Integer.parseInt(properties.getProperty("clientPort"));
            int raftPort = Integer.parseInt(properties.getProperty("raftPort"));
            Server server = new Server();
            server.start(peers, nodeId , clientPort , raftPort);

        }catch(Exception e){
            e.printStackTrace();
        }
        
    }
    
    public void start(ArrayList<String> peers, String nodeId, int clientPort,int raftPort) throws IOException{
        // selector to notify about new connections
        if(peers.size() == 0) throw new IllegalArgumentException("No peers provided");
        if(nodeId == null) throw new IllegalArgumentException("No nodeId provided");
        cache = new Cache();
        raftTransport = new SocketRaftTransport();
        raftNode = new RaftNode(peers, nodeId, cache , raftTransport);
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
        LOGGER.info("Accepted connection {} ", socket.getInetAddress());
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
            LOGGER.info("reading character : {} ", c);
            commandLine.append(c);
        }
        // below code ensure that we read \n after \r and \n is not part of command
        if(buffer.hasRemaining()) buffer.get();
        Command command = null;
        byte[] valueBytes = null;
        System.out.println("command from client: " + commandLine.toString());
        try{
            command = CommandParser.parse(commandLine.toString());
            LOGGER.info("command from client: {}", command);
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
            LOGGER.info("Error while parsing command : {} ", e.getMessage());
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
                e.printStackTrace();
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
            if(command.getType() == CommandType.GET){
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
                LOGGER.info("response from raft node : {}", response);
                return response.getBytes();
            }                
        } catch (Exception e) {
            // TODO: handle exception
            e.printStackTrace();
            return "ERROR\r\n".getBytes();
            
        }
    }

    public void write(SelectionKey key) throws IOException {
        
        Map.Entry<SelectionKey, ByteBuffer> entry = pendingWrites.poll();
        if(entry == null) return;
        SelectionKey  selectionKey = entry.getKey();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer responseBuffer = entry.getValue();
        LOGGER.info("writing response to client {} ", new String(responseBuffer.array(), 0, responseBuffer.limit()));
        while(responseBuffer.hasRemaining()){
            socketChannel.write(responseBuffer);
        }// making selector ready for read operation
        selectionKey.interestOps(SelectionKey.OP_READ);
    }
    
}
