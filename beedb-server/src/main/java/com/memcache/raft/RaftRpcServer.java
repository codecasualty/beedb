package com.memcache.raft;
// this class will be responsible for handling all the rpc requests from one node to another

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;

import org.slf4j.MDC;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;

// raftrpcserver is purely receiver side, it will be listening for requests from other nodes
// and will be handling those requests
public class RaftRpcServer {
    
    // our server is listening on port 11211
    // we will need port which will be used by our server to send request to other nodes
    
    ServerSocket serverSocket;

    /**
     * NOTE: We receive client connection requests from other nodes, and on
     * each request, we are creating a new thread to handle that request.
     * This creates a socket connection and closes it once our work is done.
     * However, here we are trying to add connection pooling.
     * <p>
     * The client maintains a map at its end, see {@link com.memcache.raft.SocketRaftTransport}
     * for implementation details. The server sets a socket timeout on its end so that the client
     * can keep it handy when needed, and the server waits/blocks itself on
     * {@code readline()} and then processes it when it receives a request from the client.
     */
    private static final int IDLE_CONNECTION_TIMEOUT_MS = 60_000;
    // to handle requests of raft node we will need reference to interact with it
    RaftNode raftNode;
    // object maper (jackson) to convert objects to json and vice versa
    ObjectMapper objectMapper;
    
    private volatile boolean stopping = false;
    private Logger LOGGER = LoggerFactory.getLogger(RaftRpcServer.class.getName());
    public RaftRpcServer(int port, RaftNode raftNode) throws IOException {

        objectMapper = new ObjectMapper();
        serverSocket = new ServerSocket(port);
        this.raftNode = raftNode;
    }
    /*
    without innner virtual threads
    accept() -> handleConnection() -> blocks on raftNode.handleRequestVote() (synchronized, may wait)
                                    meanwhile, no new connections accepted
                                    
    with inner virtual threads
    accept() → hand off to virtual thread → immediately back to accept()
                virtual thread handles the blocking work independently

     */
    public void start() {
        // open a server socket to listen for requests from other nodes
        Thread.ofVirtual().start(() -> {
            while(!stopping){
                // accept connection from other nodes
                // we will have to handle requests from other nodes
                try {
                    // so we will have to create a new thread to handle that request
                    // we will have to read request from socket
                    // and convert it to object
                    // then we will have to pass that object to raft node
                    // and raft node will handle that request
                    // once response is available we will have to convert it to json
                    // and send it back to other node
                    Socket socket = serverSocket.accept();
                    // bounds how long a handler will block waiting for the NEXT request
                    socket.setSoTimeout(IDLE_CONNECTION_TIMEOUT_MS);
                    Thread.ofVirtual().start(() -> {
                        try(Socket s = socket) {
                            handleConnection(s);
                            LOGGER.debug("connection will be closed from source node {}", socket.getInetAddress());
                            LOGGER.debug("connection closed from source node {}", socket.getInetAddress());
                            } catch (Exception e) {
                                LOGGER.error("something is wrong {}",e);
                            }
                        });
                } catch (Exception e) {
                    if(stopping){
                        LOGGER.debug("stopping rpc server");
                    }else{
                        LOGGER.error("something is wrong {}",e);
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e1) {
                            LOGGER.error("someone disturbed rpc server while sleeping :( {}",e1);
                        }
                    }
                }
            }
        });
    }

    /*
    JSON envelope structure
    {"rpcType":"REQUEST_VOTE","payload":{...}}
    {"rpcType":"APPEND_ENTRIES","payload":{...}}

     */
    /**
     * Serve requests on one connection until the peer hangs up, the connection
     * goes idle, or something goes wrong.
     *
     * Previously this handled exactly ONE request and returned, so the caller
     * closed the socket immediately. That made client-side connection pooling
     * useless: the peer pooled a connection the server had already closed, and
     * paid a fresh TCP handshake on every RPC anyway. Connection reuse is a
     * two-sided change -- the server has to keep the connection open.
     */
    private void handleConnection(Socket socket) throws IOException {

        // ONE reader for the whole connection, created outside the loop.
        // A BufferedReader per request would be a bug: it reads ahead, so bytes
        // belonging to request N+1 can already be sitting inside reader N, and a
        // fresh reader would never see them.
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        while(true){

            if( raftNode.serviceShuttingDown()){
                LOGGER.debug("service is shutting down , closing connection from source node {}", socket.getInetAddress());
                return;
            }
            // we will have to read request from socket depending on type of request
            // if its a request to append entries to log then we will have to read request from socket
            // and then call append entries method of raft node
            // once response is available we will have to convert it to json
            // and send it back to other node
            // similarly if we have request asking for vote then we will have to read request from socket
            // and then call handle request vote method of raft node
            // once response is available we will have to convert it to json
            // and send it back to other node

            String request;
            try{
                request = bufferedReader.readLine();
            }catch(SocketTimeoutException e){
                // nothing arrived for IDLE_CONNECTION_TIMEOUT_MS. The peer may be gone
                // without having closed (kill -9, partition, crash), so reclaim the
                // thread and the file descriptor. Expected, not an error.
                LOGGER.debug("connection from {} idle, reclaiming", socket.getInetAddress());
                return;
            }

            // null means end of stream: the peer closed its side. That is the NORMAL
            // way a pooled connection ends -- on peer shutdown, or when the peer
            // discards this connection from its own pool. Never an error.
            if(request == null){
                LOGGER.debug("peer {} closed the connection", socket.getInetAddress());
                return;
            }

            LOGGER.debug("received request from source node {} : {}", socket.getInetAddress(), request);
            JsonNode root = objectMapper.readTree(request);
            String rpcType = root.get("rpcType").asText();
            LOGGER.debug("received rpc type from source node {} : {}", socket.getInetAddress(), rpcType);
            // we have to get payload from map
            JsonNode payload = root.get("payload");
            if(rpcType.equals("REQUEST_VOTE")){
                // we have to read request from socket
                // but we need to read only payload from map , not the whole map
                // so we have to get payload from map
                // converting payload
                RequestVoteRequest requestVoteRequest = objectMapper.treeToValue(payload, RequestVoteRequest.class);
                // we have to pass that object to raft node 
                RequestVoteResponse response = raftNode.handleRequestVote(requestVoteRequest);
                // once response is available we will have to convert it to json 
                // and send it back to other node
                String responseString = objectMapper.writeValueAsString(response);
                LOGGER.debug("sending response to peer node {} : {}", socket.getInetAddress(), responseString);
                sendResponse(socket, responseString);
            }else if (rpcType.equals("APPEND_ENTRIES")){
                // we have to read request from socket
                AppendEntriesRequest requestAppendEntries = objectMapper.treeToValue(payload , AppendEntriesRequest.class);
                // we have to pass that object to raft node
                AppendEntriesResponse response = raftNode.handleAppendEntries(requestAppendEntries);
                // once response is available we will have to convert it to json 
                // and send it back to other node
                String responseString = objectMapper.writeValueAsString(response);
                LOGGER.debug("sending response to peer node {} : {}", socket.getInetAddress(), responseString);
                sendResponse(socket, responseString);
            }else if(rpcType.equals("INSTALL_SNAPSHOT")){
                InstallSnapshotRequest installSnapshotRequest = objectMapper.treeToValue(payload, InstallSnapshotRequest.class);
                InstallSnapshotResponse response = raftNode.handleInstallSnapshot(installSnapshotRequest);
                String responseString = objectMapper.writeValueAsString(response);
                LOGGER.debug("sending response to peer node {} : {}", socket.getInetAddress(), responseString);
                sendResponse(socket, responseString);
            }

            // One thread now serves many requests, so per-request MDC state must not
            // leak into the next one. The handlers set requestId themselves; this
            // makes sure a request that never set it is not logged under the last
            // request's id.
            MDC.clear();

        }
    }

    private void sendResponse(Socket socket, String response) throws IOException{
        // we will have to convert response to json
        // and send it back to other node
        // we will have to write response to socket
        response += "\r\n";
        socket.getOutputStream().write(response.getBytes());
        socket.getOutputStream().flush();
    }

    public void close(){
        try {
            stopping = true;
            serverSocket.close();
        } catch (IOException e) {
            LOGGER.debug("Closing socket, some exception is thrown {}", e);
        }
    }

}
