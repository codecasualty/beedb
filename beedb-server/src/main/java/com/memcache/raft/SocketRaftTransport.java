package com.memcache.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.memcache.raft.rpc.AppendEntriesRequest;
import com.memcache.raft.rpc.AppendEntriesResponse;
import com.memcache.raft.rpc.InstallSnapshotRequest;
import com.memcache.raft.rpc.InstallSnapshotResponse;
import com.memcache.raft.rpc.RequestVoteRequest;
import com.memcache.raft.rpc.RequestVoteResponse;

public class SocketRaftTransport implements RaftTransport {

    private final static Logger LOGGER = LoggerFactory.getLogger(SocketRaftTransport.class.getName());

    // Metrics go to a dedicated "METRICS" logger, not the class logger, so they can be
    // switched on for a benchmark without also enabling this class's DEBUG output.
    // logback.xml has it OFF by default
    private static final Logger METRICS = LoggerFactory.getLogger("METRICS");

    // object mapper (jackson) to convert objects to json and vice versa
    private ObjectMapper objectMapper = new ObjectMapper();

    /*
     * timing convention used by every method below:
     *
     *   t0        - taken once, before the socket is constructed. Everything is
     *               measured relative to this Us means micro seconds
     *   connectUs - the TCP three-way handshake (the Socket constructor).
     *   writeUs   - write + flush. write only copies into the kernel
     *               send buffer, it does not wait for the peer
     *   readUs    - readLine + parse. 
     *               This is the number setSoTimeout is cutting off.
     *   totalUs   - measured from t0. totalUs - (connect+write+read) is the
     *               Jackson serialize/deserialize cost
     *   connectionPoolMap - keeps track of all the sockets which are used to communicate with other nodes
     *   peers - list of all the peers
     *   so each peer will have this connection pool the structure is like this
     *   so for peer1, which for our example is 127.0.0.1:11211 , we will have socket2, socket3 and socket4 in the connection pool
     *   connection pool does not mean we have to create eagerly , it means we should have a previously used socket to reduce/remove the connection creation overhead
     *   connectionPoolMap = {
     *       peer2 = [socket1, socket2, socket3]
     *       peer3 = [socket1, socket2, socket3]
     *   }
     */
    private final List<String> peers;
    private final ConcurrentHashMap<String , BlockingQueue<Socket>> connectionPoolMap = new ConcurrentHashMap<>();
    private volatile boolean isShuttingDown = false;
    public SocketRaftTransport(List<String> peers) {
        this.peers = peers;
        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
    }
    
    @Override
    public RpcResult<RequestVoteResponse> sendRequestVoteToPeer(RequestVoteRequest request, String peer) {
        long currentTerm = request.getTerm();
        String nodeId = request.getCandidateId();
        

        String[] address = peer.split(":");
        int port = Integer.parseInt(address[1]);
        String ip = address[0]; 
        RequestVoteResponse response = null;
        // here one thing to note is that , what if our peer is down or unreachable, in that case socket.close() wont make sense and will throw error
        // so we will use conventional java pattern for resource cleanup 
        // Socket implements closeable so no need to explicitly close it
        LOGGER.debug("term {} node id {} vote request to peer {} and request {} ", currentTerm, nodeId, peer, request);
        long t0 = System.nanoTime();
        long connectUs = -1, writeUs = -1, readUs = -1;
        String phase = "connect";
        Socket socket = takeConnection(peer);
        if(socket == null) return RpcResult.unreachable("Socket is null");
        try{
            socket.setSoTimeout(200);
            connectUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0);
            phase = "write";
            // to create a json envelope
            Map<String , Object> envelope = Map.of(
                "rpcType" , "REQUEST_VOTE",
                "payload" , request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            long w0 = System.nanoTime();
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            writeUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - w0);
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // we can use buffered reader to read response from socket as readline will read till \n or \r\n so its better to use it
            phase = "read";
            long r0 = System.nanoTime();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = bufferedReader.readLine();
            RequestVoteResponse requestVoteResponse = objectMapper.readValue(line, RequestVoteResponse.class);
            readUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - r0);
            LOGGER.debug("term {}  node id {} read from socket {} and response {} ", currentTerm, nodeId, socket, requestVoteResponse);
            METRICS.info("METRIC rpc type=REQUEST_VOTE peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            if(!socket.isClosed() && socket.isConnected())
            putBackConnection(socket, peer);
            return RpcResult.ok(requestVoteResponse);
        }catch(SocketTimeoutException e){
            LOGGER.debug("term {} node id {} and follower is {} request is {} ", currentTerm, nodeId, peer, request);
            LOGGER.debug("Exception in sendRequestVoteToPeer", e);
            METRICS.info("METRIC rpc type=REQUEST_VOTE peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);

            return RpcResult.timeout(e.getClass().getSimpleName());
        }catch(SocketException e){
            LOGGER.debug("term {} node id {} and follower is {} request is {} ", currentTerm, nodeId, peer, request);
            LOGGER.debug("Exception in sendRequestVoteToPeer", e);
            METRICS.info("METRIC rpc type=REQUEST_VOTE peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);

            
            return RpcResult.unreachable(e.getClass().getSimpleName());
        }
        catch(Exception e){
            LOGGER.debug("term {} node id {} and follower is {} request is {} ", currentTerm, nodeId, peer, request);
            LOGGER.debug("Exception in sendRequestVoteToPeer", e);
            METRICS.info("METRIC rpc type=REQUEST_VOTE peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);

            
            return RpcResult.badResponse(e.getClass().getSimpleName());
        }
    }

    @Override
    public RpcResult<AppendEntriesResponse> sendAppendEntriesToPeer(AppendEntriesRequest request, String peer) {
        long currentTerm = request.getTerm();
        String nodeId = request.getLeaderId(); 
        // the node which is sending the append entries is leader
        String leaderId = nodeId;

        String[] address = peer.split(":");
        int port = Integer.parseInt(address[1]);
        String ip = address[0];
        AppendEntriesResponse response = null;
        LOGGER.debug("term {} node id {} append entries request {} to peer {} ", currentTerm, nodeId, request, peer);
        long t0 = System.nanoTime();
        long connectUs = -1, writeUs = -1, readUs = -1;
        String phase = "connect";
        Socket socket = takeConnection(peer);
        if(socket == null) return RpcResult.unreachable("Socket is null");
        try{
            socket.setSoTimeout(200);
            connectUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0);
            phase = "write";
            // first make json envelope
            // jackson can serialize appendentriesrequest automatically because it has getters
            Map<String , Object> envelope = Map.of(
                "rpcType" , "APPEND_ENTRIES",
                "payload", request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            // write this into sockets output stream 
            long w0 = System.nanoTime();
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            writeUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - w0);
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // now we will receive respose from nodes , read it 
            phase = "read";
            long r0 = System.nanoTime();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String socketResponse = reader.readLine();
            response = objectMapper.readValue(socketResponse, AppendEntriesResponse.class);
            readUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - r0);
            LOGGER.debug("term {} node id {} read from socket {} and request {} ", currentTerm, nodeId, socket, request);
            METRICS.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            if(!socket.isClosed() && socket.isConnected())
            putBackConnection(socket, peer);
            return RpcResult.ok(response);

        }catch(SocketTimeoutException e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, leaderId, peer, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeer", e);
            METRICS.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);
            return RpcResult.timeout(e.getClass().getSimpleName());
        }
        catch(SocketException e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, leaderId, peer, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeer", e);
            METRICS.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);
            return RpcResult.unreachable(e.getClass().getSimpleName());
        }
        catch(Exception e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, leaderId, peer, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeer", e);
            METRICS.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);

            return RpcResult.badResponse(e.getClass().getSimpleName());
        }
    }

    @Override
    public RpcResult<InstallSnapshotResponse> sendInstallSnapshotToPeer(InstallSnapshotRequest request, String peer) {
        long currentTerm = request.getTerm();
        String nodeId = request.getLeaderId();
        String followerId = peer;
        InstallSnapshotResponse response = null;
        String[] address = peer.split(":");
        int port = Integer.parseInt(address[1]);
        String ip = address[0];
        LOGGER.debug("term {} node id {} install snapshot request {} to peer {} ", currentTerm, nodeId, request, peer);
        long t0 = System.nanoTime();
        long connectUs = -1, writeUs = -1, readUs = -1;
        String phase = "connect";
        Socket socket = takeConnection(peer);
        if(socket == null) return RpcResult.unreachable("Socket is null");
        try{
            socket.setSoTimeout(200);
            connectUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0);
            phase = "write";
            // first make json envelope
            // jackson can serialize appendentriesrequest automatically because it has getters
            Map<String , Object> envelope = Map.of(
                "rpcType" , "INSTALL_SNAPSHOT",
                "payload", request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            // write this into sockets output stream 
            long w0 = System.nanoTime();
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            writeUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - w0);
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // now we will receive respose from nodes , read it 
            phase = "read";
            long r0 = System.nanoTime();
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String socketResponse = reader.readLine();
            response = objectMapper.readValue(socketResponse, InstallSnapshotResponse.class);
            readUs = TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - r0);
            LOGGER.debug("term {} node id {} read from socket {} and request {} ", currentTerm, nodeId, socket, request);
            METRICS.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            if(!socket.isClosed() && socket.isConnected())
            putBackConnection(socket, peer);
            return RpcResult.ok(response);

        }catch(SocketTimeoutException e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, nodeId, followerId, request);
            LOGGER.debug("Exception in sendInstallSnapshotToPeer", e);
            METRICS.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket, peer);
            return RpcResult.timeout(e.getClass().getSimpleName());
        }
        catch(SocketException e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, nodeId, followerId, request);
            LOGGER.debug("Exception in sendInstallSnapshotToPeer", e);            
            METRICS.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket ,peer);
            return RpcResult.unreachable(e.getClass().getSimpleName());
        }
        catch(Exception e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, nodeId, followerId, request);
            LOGGER.debug("Exception in sendInstallSnapshotToPeer", e);
            METRICS.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
            returnNewConnection(socket , peer); 
            return RpcResult.badResponse(e.getClass().getSimpleName());
        }
    }

    public void close() {
        isShuttingDown = true;
        connectionPoolMap.clear();
        // try{
        //     connectionPoolMap.forEach((k,v) -> {
        //             try{
        //             v.forEach(s -> {
        //                 try {
        //                     s.close();
        //                 } catch (IOException e) {
        //                     LOGGER.error("something is wrong ",e);
        //                     LOGGER.debug("closing socket , please check stack trace ", e);
        //                 }
        //             });
        //         }catch(Exception e){
        //              LOGGER.error("something is wrong ",e);
        //                     LOGGER.debug("closing socket , please check stack trace ", e);
        //         }
        //     });
        
        // }catch(Exception e){
        //     LOGGER.error("something is wrong ",e);
        //             LOGGER.debug("closing socket , please check stack trace ", e);
        // }
    }

    public Socket createNewConnection(String ip , int port){
        LOGGER.debug("creating new connection to {} port {} ", ip, port);
        if(isShuttingDown) return null;
        try{
            Socket socket = new Socket(ip, port);
            socket.setSoTimeout(200);
            return socket;
        }catch(Exception e){
            LOGGER.debug("something is wrong ",e);
            LOGGER.debug("closing socket , please check stack trace ", e);
            return null;
        }
    }

    public Socket createNewConnection(String peer){
        String[] address = peer.split(":");
        int port = Integer.parseInt(address[1]);
        String ip = address[0];
        return createNewConnection(ip , port);
    }

    public void putBackConnection(Socket socket, String peer){
        if(socket == null || socket.isClosed() || connectionPoolMap.get(peer).contains(socket)) return ;
        connectionPoolMap.get(peer).add(socket);
    }

    public Socket takeConnection(String peer){
        if(!connectionPoolMap.containsKey(peer))connectionPoolMap.putIfAbsent(peer, new LinkedBlockingQueue<>());
        try{
            Socket socket = connectionPoolMap.computeIfAbsent(peer, k -> new LinkedBlockingQueue<>()).poll();
            if(socket == null){
                socket = createNewConnection(peer);
                LOGGER.debug("Socket is created successfully and socket is {} ", socket);
            } 
            return socket;
            
        }catch(Exception e){
            LOGGER.debug("something is wrong ",e);
            LOGGER.debug("closing socket , please check stack trace ", e);
        }
        return null;
    }


    public void returnNewConnection(Socket socket, String peer){
        try{
            socket.close();
            // returnNewConnection(peer);
        }catch(Exception e){
            LOGGER.debug("something is wrong {}",e);
        }
    }
}


