package com.memcache.raft;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.Map;

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
        try(Socket socket = new Socket(ip, port)){
            socket.setSoTimeout(100);
            // to create a json envelope
            Map<String , Object> envelope = Map.of(
                "rpcType" , "REQUEST_VOTE",
                "payload" , request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // we can use buffered reader to read response from socket as readline will read till \n or \r\n so its better to use it
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line = bufferedReader.readLine();
            RequestVoteResponse requestVoteResponse = objectMapper.readValue(line, RequestVoteResponse.class);
            LOGGER.debug("term {}  node id {} read from socket {} and response {} ", currentTerm, nodeId, socket, requestVoteResponse);
            return requestVoteResponse;
        }catch(Exception e){
            LOGGER.error("print stacktrace", e);
            LOGGER.debug("term {} node id {} and follower is {} request is {} ", currentTerm, nodeId, peer, request);
            LOGGER.debug("Exception in sendRequestVoteToPeer", e);
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
        try(Socket socket = new Socket(ip , port)){
            socket.setSoTimeout(100);

            // first make json envelope
            // jackson can serialize appendentriesrequest automatically because it has getters
            Map<String , Object> envelope = Map.of(
                "rpcType" , "APPEND_ENTRIES",
                "payload", request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            // write this into sockets output stream 
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // now we will receive respose from nodes , read it 
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String socketResponse = reader.readLine();
            response = objectMapper.readValue(socketResponse, AppendEntriesResponse.class);
            LOGGER.debug("term {} node id {} read from socket {} and request {} ", currentTerm, nodeId, socket, request);
            return response;

        }catch(Exception e){
            LOGGER.error("print stacktrace", e);

            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, leaderId, peer, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeerInParallel", e);
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
        try(Socket socket = new Socket(ip , port)){
            socket.setSoTimeout(100);
            // first make json envelope
            // jackson can serialize appendentriesrequest automatically because it has getters
            Map<String , Object> envelope = Map.of(
                "rpcType" , "INSTALL_SNAPSHOT",
                "payload", request
            );
            String json = objectMapper.writeValueAsString(envelope)+"\r\n";
            // write this into sockets output stream 
            socket.getOutputStream().write(json.getBytes());
            socket.getOutputStream().flush();
            LOGGER.debug("term {} node id {} written to socket {} and request {} ", currentTerm, nodeId, socket, request);
            // now we will receive respose from nodes , read it 
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String socketResponse = reader.readLine();
            response = objectMapper.readValue(socketResponse, InstallSnapshotResponse.class);
            LOGGER.debug("term {} node id {} read from socket {} and request {} ", currentTerm, nodeId, socket, request);
            return response;

        }catch(Exception e){
            LOGGER.error("print stacktrace", e);

            LOGGER.debug("term {} node id {} leader is {} and follower is {} request is {} ", currentTerm, nodeId, nodeId, followerId, request);
            LOGGER.debug("Exception in sendAppendEntriesToPeerInParallel", e);
        }
        return response;
    }

}
