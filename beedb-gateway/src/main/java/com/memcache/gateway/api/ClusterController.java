package com.memcache.gateway.api;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.memcache.gateway.cluster.ClusterStatusService;
import com.memcache.gateway.cluster.ClusterView;

@RestController
@RequestMapping("/api")
public class ClusterController {
    
    private final ClusterStatusService clusterStatusService;
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterController.class);
    
    public ClusterController(ClusterStatusService clusterStatusService){
        this.clusterStatusService = clusterStatusService;
    }
    
    @GetMapping ("/cluster")
    public ResponseEntity<ClusterView> getClusterStatus(){
        ClusterView clusterView = clusterStatusService.getClusterView();
        return ResponseEntity.ok(clusterView);
    }

    @GetMapping (value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getClusterStatusEvents(){
        try{
            SseEmitter emitter = clusterStatusService.registerEmitter();
            return emitter;
        }catch(Exception e){
            LOGGER.error("failed to register emitter", e);
            return null;
        }
    }
}
