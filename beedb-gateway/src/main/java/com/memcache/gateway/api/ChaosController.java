package com.memcache.gateway.api;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.memcache.gateway.chaos.ChaosKillState;
import com.memcache.gateway.chaos.ChaosService;
import com.memcache.gateway.cluster.ClusterSnapshot;
import com.memcache.gateway.cluster.ClusterStatusService;

@RestController 
@RequestMapping ("/api/chaos")
public class ChaosController {
    
    private final ClusterStatusService clusterStatusService;
    private final ChaosService chaosService;
    public ChaosController(ClusterStatusService clusterStatusService, ChaosService chaosService){
        this.clusterStatusService = clusterStatusService;
        this.chaosService = chaosService;
    }

    @PostMapping ("/kill")
    public ResponseEntity<String> kill(){
        ClusterSnapshot clusterSnapshot = clusterStatusService.getClusterSnapshot();
        ChaosKillState state = chaosService.requestKill(clusterSnapshot.leaderId(), clusterSnapshot);
        if(state == ChaosKillState.SUCCESS){
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(state.toString());
        } else if (state == ChaosKillState.UNDER_COOLDOWN) {
            long secondsUntilKill = chaosService.secondsUntilEligible();
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                                  .header("Retry-After", String.valueOf(secondsUntilKill))
                                  .body(state.toString());
        } else {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(state.toString());
        }
        
    }
}
