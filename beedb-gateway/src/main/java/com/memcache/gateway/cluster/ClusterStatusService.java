package com.memcache.gateway.cluster;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.memcache.gateway.chaos.ChaosService;
import com.memcache.gateway.client.BeedbClient;
import com.memcache.gateway.demo.DemoWriter;

@Service 
public class ClusterStatusService {
    
    volatile ClusterSnapshot clusterSnapshot;
    private final BeedbClient beedbClient;
    private final List<SseEmitter> emitters  = new CopyOnWriteArrayList<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStatusService.class);
    private final ChaosService chaosService;
    volatile ClusterView clusterView;
    private final DemoWriter demoWriter;
    public ClusterStatusService(BeedbClient beedbClient, ChaosService chaosService, DemoWriter demoWriter){
        this.beedbClient = beedbClient;
        this.chaosService = chaosService;
        this.demoWriter = demoWriter;
        this.clusterView = new ClusterView(null, chaosService.getChaosView() , demoWriter.view());
    }
    public ClusterSnapshot getClusterSnapshot(){
        if(clusterSnapshot == null){
            clusterSnapshot = new ClusterSnapshot(new ArrayList<>(), null, false, Instant.now());
        }
        return clusterSnapshot;
    }

    public SseEmitter registerEmitter(){
        // to avoid 30s timeout
        SseEmitter emitter = new SseEmitter(Long.MAX_VALUE);
        emitter.onError(throwable -> {
            emitters.remove(emitter);
        });
        emitter.onTimeout(() -> {
            emitters.remove(emitter);
        });
        emitter.onCompletion(() ->{
            emitters.remove(emitter);
        });
        emitters.add(emitter);  
        try {
            emitter.send(clusterView);
        } catch (Exception e) {
            logFailure(e, "failed to send cluster status");
        }
        return emitter;
    }

    public void sendClusterStatus(){
        LOGGER.debug("sending cluster status");
        emitters.forEach(emitter -> {
            try {
                emitter.send(clusterView);
                LOGGER.debug("sent cluster status");
            } catch (Exception e) {
                emitters.remove(emitter);
                logFailure(e , "failed to send cluster status");
            }
        });
    }

    public void updateClusterStatus(){
        try{
            // LOGGER.debug("updating cluster status");
            List<NodeStatus> nodes = beedbClient.getClusterStatus();
            int reachableNodes = 0;
            String leaderId = null;
            long currentTerm = -1;
            for(NodeStatus nodeStatus : nodes){
                if(nodeStatus.raftRole() == null || nodeStatus.raftTerm() == null) {
                    continue;
                }
                reachableNodes++;
                if("LEADER".equals(nodeStatus.raftRole()) && nodeStatus.raftTerm() > currentTerm){
                    leaderId = nodeStatus.raftNodeId();
                    currentTerm = nodeStatus.raftTerm();
                }
            }
            this.clusterSnapshot = new ClusterSnapshot(nodes , leaderId , reachableNodes > (nodes.size() / 2) , Instant.now());
            LOGGER.debug("updated cluster status at {}" , Instant.now());
        }catch(Exception e){
            logFailure(e, null);
        }
    }
    
    
    @Scheduled (fixedDelay = 1000)
    public void scheduledUpdateClusterStatus(){
        updateClusterStatus();
        // sending updated cluster status to all connected clients
        try{
            chaosService.tick(clusterSnapshot);
        }catch(Exception e){
            logFailure(e, "failed to tick chaos service");
        }
        clusterView = new ClusterView(clusterSnapshot, chaosService.getChaosView(), demoWriter.view());
        LOGGER.debug("cluster status sent to all connected clients");
        sendClusterStatus();
    }

    private void logFailure(Exception e, String message){
        if(e instanceof java.net.ConnectException || e instanceof java.net.SocketException){
            LOGGER.warn("{} ({})", message , e.getMessage());
        }else if(e instanceof java.net.SocketTimeoutException){
            LOGGER.warn("{} ({})", message , e.getMessage());
        }else{
            LOGGER.error("{} ", message , e);
        }
    }

    public ClusterView getClusterView(){
        return new ClusterView(clusterSnapshot, chaosService.getChaosView() ,demoWriter.view());
    }

}
