/*
* Main Server class for memcache server
* Author : Subodh
* Date   : 2022-01-10
* mail   : trycodeforfun@gmail.com
 */

package com.memcache;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Iterator;
import java.util.Set;
import java.nio.channels.SocketChannel;
import java.security.Key;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
public class Server {

    ConcurrentLinkedQueue<Map.Entry<SelectionKey, ByteBuffer>> pendingWrites = new ConcurrentLinkedQueue<>();

    public static void main(String[] args) throws IOException{

        Server server = new Server();
        server.start();
        
    }
    
    public void start() throws IOException{
        // selector to notify about new connections
        Selector selector = Selector.open();
        // server socket to listen for new connections
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.bind(new InetSocketAddress(11211));
        serverSocketChannel.configureBlocking(false);
        serverSocketChannel.register(selector , SelectionKey.OP_ACCEPT);
    
        ExecutorService executorService = Executors.newFixedThreadPool(10);
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
        System.out.println("Accepted connection "+ socket.getInetAddress());
    }


    private static final int MAX_BODY_SIZE = 1024 * 1024; // 1 MB

    public void read(Selector selector, SelectionKey key, ExecutorService executorService) throws IOException{
        SocketChannel socketChannel = (SocketChannel) key.channel();

        ByteBuffer headerBuffer = ByteBuffer.allocate(4);
        int headerRead = socketChannel.read(headerBuffer);
        if(headerRead == -1){
            key.cancel();
            socketChannel.close();
            return;
        }
        if(headerBuffer.position() < 4) return;

        headerBuffer.flip();
        int expectedSize = headerBuffer.getInt();

        if(expectedSize <= 0 || expectedSize > MAX_BODY_SIZE){
            System.out.println("Invalid request");
            key.cancel();
            socketChannel.close();
            return;
        }

        ByteBuffer bodyBuffer = ByteBuffer.allocate(expectedSize);
        while(bodyBuffer.hasRemaining()){
            int bodyRead = socketChannel.read(bodyBuffer);
            if(bodyRead == -1) break;
            if(bodyRead == 0) continue;
        }
        bodyBuffer.flip();
        System.out.println("request from client: " + new String(bodyBuffer.array(), 0, bodyBuffer.limit()));

        key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);
        byte[] data = bodyBuffer.array();
        executorService.execute(() -> {
            byte[] response = processRequest(data);
            ByteBuffer responseBuffer = ByteBuffer.wrap(response);
            pendingWrites.add(Map.entry(key, responseBuffer));
            key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
            selector.wakeup();
        });
    }

    public byte[] processRequest(byte[] data){
        // we need to return the response as length of data and data itself
        byte[] response = new byte[data.length + 4];
        ByteBuffer buffer = ByteBuffer.wrap(response);
        buffer.putInt(data.length);
        buffer.put(data);
        buffer.flip();
        return response;
    }

    public void write(SelectionKey key) throws IOException {
        
        Map.Entry<SelectionKey, ByteBuffer> entry = pendingWrites.poll();
        if(entry == null) return;
        SelectionKey  selectionKey = entry.getKey();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer responseBuffer = entry.getValue();
        System.out.println("writing response to client "+ new String(responseBuffer.array(), 0, responseBuffer.limit()));
        while(responseBuffer.hasRemaining()){
            socketChannel.write(responseBuffer);
        }// making selector ready for read operation
        selectionKey.interestOps(SelectionKey.OP_READ);
    }
    
}
