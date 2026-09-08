package com.memcache.gateway.chaos;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.gateway.chaos.chaosexception.ChaosException;
import com.memcache.gateway.cluster.ClusterSnapshot;
import com.memcache.gateway.cluster.NodeStatus;
import com.memcache.gateway.supervisor.NodeSupervisor;

public class ChaosService {
    private final AtomicReference<ChaosState> chaosState = new AtomicReference<>();
    private final NodeSupervisor nodeSupervisor;
    private final Clock clock;
    private final int announceLeadSeconds;
    private final int autoKillSeconds;
    private final int cooldownSeconds;
    private static final int STARTING_SECONDS = 60;
    private static final Logger LOGGER = LoggerFactory.getLogger(ChaosService.class);
    public ChaosService(NodeSupervisor nodeSupervisor, int announceLeadSeconds, int autoKillSeconds, int cooldownSeconds, Clock clock){

        this.nodeSupervisor = nodeSupervisor;
        Instant currentTime = clock.instant();
        chaosState.set(ChaosState.idle(currentTime.plus(STARTING_SECONDS, ChronoUnit.SECONDS),
        currentTime.plus(autoKillSeconds, ChronoUnit.SECONDS)));
        this.clock = clock;
        this.announceLeadSeconds = announceLeadSeconds;
        this.autoKillSeconds = autoKillSeconds; 
        this.cooldownSeconds = cooldownSeconds;
    }

    public ChaosState getState(){
        return chaosState.get();
    }

    public ChaosState tick(ClusterSnapshot clusterSnapshot){

        ChaosState currentState = chaosState.get();
        boolean isHealthyCluster = healthyCluster(clusterSnapshot);
        Instant currentInstant = clock.instant();
        if(isHealthyCluster && currentState.phase().equals(ChaosPhase.IDLE)){
            if(!currentInstant.isAfter(currentState.nextEligibleAt()) || !currentInstant.isAfter(currentState.nextAutoAt())){
                return currentState;
            }
            transitionIdleAnnounced(clusterSnapshot.leaderId(), currentInstant);
        }
        else if(currentState.phase().equals(ChaosPhase.ANNOUNCED) && currentInstant.isAfter(currentState.killAt())){
            transitionAnnouncedKilled(currentState.targetNodeId());
        }
        else if(currentState.phase().equals(ChaosPhase.KILLED)){
            for(NodeStatus nodeStatus : clusterSnapshot.nodes()){
                if(nodeStatus.raftNodeId().equals(currentState.targetNodeId())){  
                    // above conditions just gurantees that the current node is same as what we choose for killing 
                    // it doesn't help us know whether it was killed or not 
                    // if the node under suspicion is alive and its was observed down that means we can move it to idle
                    if(nodeStatus.raftRole() != null && currentState.observedDown()){
                        transitionKilledIdle(currentState.targetNodeId(), currentInstant);
                    }
                    // the node is dead and we haven't observed it down yet, so its to make sure in next iteration we observe
                    // it down
                    else if(nodeStatus.raftRole() == null && !currentState.observedDown()){
                        ChaosState newState = ChaosState.killed(currentState.targetNodeId(), currentState.nextEligibleAt(), currentState.nextAutoAt(), true);
                        chaosState.compareAndSet(currentState, newState);
                    }
                    break;
                }
            }
        }

        return chaosState.get();

    }

    public ChaosKillState requestKill(String nodeId, ClusterSnapshot clusterSnapshot){
        ChaosState currentState = chaosState.get();
        Instant nextEligibleAt = currentState.nextEligibleAt();
        Instant currentInstant = clock.instant();
        if(!currentInstant.isAfter(nextEligibleAt)){
            return ChaosKillState.UNDER_COOLDOWN;
        }else if( !healthyCluster(clusterSnapshot)){
            return ChaosKillState.CLUSTER_DEGRADED;
        }else if(currentState.phase().equals(ChaosPhase.KILLED)){
            return ChaosKillState.ALREADY_KILLED;
        }else if(currentState.phase().equals(ChaosPhase.ANNOUNCED)){
            return ChaosKillState.KILLING_TIME_ANNOUNCED;
        }

        boolean isTransitionIdleAnnounced = transitionIdleAnnouncedRequestKill(nodeId, currentInstant, currentState);
        return isTransitionIdleAnnounced ? ChaosKillState.SUCCESS : ChaosKillState.FAILED;
    }
    
    public boolean transitionIdleAnnouncedRequestKill(String nodeId, Instant currentInstant, ChaosState currentState){
        Instant currStateNextEligibleAt = currentState.nextEligibleAt();
        Instant currentStateNextAutoAt = currentState.nextAutoAt();
        Instant killAt = currentInstant.plus(announceLeadSeconds, ChronoUnit.SECONDS);
    
        ChaosState newChaosState = ChaosState.announced(nodeId, currStateNextEligibleAt, currentStateNextAutoAt, killAt);
        return chaosState.compareAndSet(currentState, newChaosState);
    }

    public boolean transitionIdleAnnounced(String nodeId, Instant currentInstant){
        // we know that our current time > nextEligibleAt but we dont know future nextEligibleAt so we 
        // not changing it , below logic is same as above and its repeaated but for readability its kept will be removed
        ChaosState currentState = chaosState.get();
        Instant currStateNextEligibleAt = currentState.nextEligibleAt();
        Instant currentStateNextAutoAt = currentState.nextAutoAt();
        Instant killAt = currentInstant.plus(announceLeadSeconds, ChronoUnit.SECONDS);
    
        ChaosState newChaosState = ChaosState.announced(nodeId, currStateNextEligibleAt, currentStateNextAutoAt, killAt);
        return chaosState.compareAndSet(currentState, newChaosState);
    }

    public boolean transitionAnnouncedKilled(String nodeId){
        ChaosState originalState = chaosState.get();
        Instant currentInstant = clock.instant();
        Instant currStateNextEligibleAt = originalState.nextEligibleAt();
        Instant currentStateNextAutoAt = originalState.nextAutoAt();
        ChaosState newChaosState = ChaosState.killed(nodeId, currStateNextEligibleAt, currentStateNextAutoAt, false);
        boolean isTransitionAnnouncedKilled = chaosState.compareAndSet(originalState, newChaosState);
        if(isTransitionAnnouncedKilled){
            try{
                nodeSupervisor.kill(nodeId);
                return true;
            }catch(ChaosException e){
                LOGGER.error("failed to kill node {}", nodeId);
                ChaosState idleState = ChaosState.idle(currentInstant.plus(cooldownSeconds, ChronoUnit.SECONDS) , currentInstant.plus(autoKillSeconds, ChronoUnit.SECONDS));
                chaosState.compareAndSet(newChaosState, idleState);
            }
        }
        return false;
    }

    public boolean transitionKilledIdle(String nodeId, Instant currentInstant){
        // dont change the state untill and unless we are sure the snapshot has that node id as back
        Instant currStateNextEligibleAt = currentInstant.plus(cooldownSeconds, ChronoUnit.SECONDS);
        Instant currentStateNextAutoAt = currentInstant.plus(autoKillSeconds, ChronoUnit.SECONDS);
        ChaosState newState = ChaosState.idle(currStateNextEligibleAt, currentStateNextAutoAt);
        ChaosState originalState = chaosState.get();
        boolean isTransitionKilledIdle = chaosState.compareAndSet(originalState, newState);
        if(isTransitionKilledIdle){
            return true;
        }
        return false;
    }


    public boolean healthyCluster(ClusterSnapshot clusterSnapshot){
        if(clusterSnapshot == null || clusterSnapshot.leaderId() == null || !clusterSnapshot.quorum() || clusterSnapshot.nodes().size() < 3){
            return false;
        }
        for(NodeStatus nodeStatus : clusterSnapshot.nodes()){
            if(nodeStatus.raftRole() == null){
                return false;
            }
        }
        return true;
    }
}
