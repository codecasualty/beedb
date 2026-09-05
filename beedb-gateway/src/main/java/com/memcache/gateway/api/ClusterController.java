package com.memcache.gateway.api;


import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import com.memcache.gateway.cluster.ClusterSnapshot;
import com.memcache.gateway.cluster.ClusterStatusService;

@RestController
@RequestMapping("/api")
public class ClusterController {
    
    private final ClusterStatusService clusterStatusService;
    
    public ClusterController(ClusterStatusService clusterStatusService){
        this.clusterStatusService = clusterStatusService;
    }
    
    @GetMapping ("/cluster")
    public ResponseEntity<ClusterSnapshot> getClusterStatus(){
        // System.out.println("getting cluster status");
        ClusterSnapshot clusterSnapshot = clusterStatusService.getClusterSnapshot();
        // System.out.println("cluster snapshot is " + clusterSnapshot);
        if(clusterSnapshot == null){
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(clusterSnapshot);
    }

    @GetMapping (value = "/events", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public SseEmitter getClusterStatusEvents(){
        System.out.println("getting cluster status events");
        SseEmitter emitter = clusterStatusService.registerEmitter();
        return emitter;
    }
}
