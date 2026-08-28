package com.memcache.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;
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

    private Logger LOGGER = LoggerFactory.getLogger(SocketRaftTransport.class.getName());

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
     */

    @Override
    public RequestVoteResponse sendRequestVoteToPeer(RequestVoteRequest request, String peer) {
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
        try(Socket socket = new Socket(ip, port)){
            socket.setSoTimeout(100);
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
            LOGGER.info("METRIC rpc type=REQUEST_VOTE peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            return requestVoteResponse;
        }catch(Exception e){
            LOGGER.debug("term {} node id {} and follower is {} request is {} ", currentTerm, nodeId, peer, request);
            LOGGER.debug("Exception in sendRequestVoteToPeer", e);
            LOGGER.info("METRIC rpc type=REQUEST_VOTE peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
        }
        return response;
    }

    @Override
    public AppendEntriesResponse sendAppendEntriesToPeer(AppendEntriesRequest request, String peer) {
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
        try(Socket socket = new Socket(ip , port)){
            socket.setSoTimeout(100);
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
            LOGGER.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            return response;

        }catch(Exception e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, leaderId, peer, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeer", e);
            LOGGER.info("METRIC rpc type=APPEND_ENTRIES peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
        }
        return response;
    }

    @Override
    public InstallSnapshotResponse sendInstallSnapshotToPeer(InstallSnapshotRequest request, String peer) {
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
        try(Socket socket = new Socket(ip , port)){
            socket.setSoTimeout(100);
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
            LOGGER.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=true connectUs={} writeUs={} readUs={} totalUs={}",
                peer, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0));
            return response;

        }catch(Exception e){
            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, nodeId, followerId, request);
            LOGGER.debug("Exception in sendInstallSnapshotToPeer", e);
            LOGGER.info("METRIC rpc type=INSTALL_SNAPSHOT peer={} ok=false phase={} connectUs={} writeUs={} readUs={} totalUs={} err={}",
                peer, phase, connectUs, writeUs, readUs, TimeUnit.NANOSECONDS.toMicros(System.nanoTime() - t0), e.getClass().getSimpleName());
        }
        return response;
    }

}
