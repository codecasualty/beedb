package com.memcache.gateway.chaos;

import java.time.Instant;

/*
* phase: idle, announced, killed
* nextEligibleAt: time when the chaos state will be eligible to be chaos
* nextAutoAt: time when the chaos will be automatically triggered
 */

public record ChaosState(
    ChaosPhase phase, 
    // IDLE state need to know when the next chaos can be triggered
    Instant nextEligibleAt,
    Instant nextAutoAt,
    // ANNOUNCED state need to know to which node is going to get killed and when
    String targetNodeId,
    Instant killAt
    // KILLED state need to know which node is killled 
){
    public static ChaosState idle(Instant nextEligibleAt, Instant nextAutoAt){
        return new ChaosState(ChaosPhase.IDLE, nextEligibleAt, nextAutoAt, null, null);
    }

    public static ChaosState announced(String targetNodeId, Instant nextEligibleAt, Instant nextAutoAt, Instant killAt){
        return new ChaosState(ChaosPhase.ANNOUNCED, nextEligibleAt, nextAutoAt, targetNodeId, killAt);
    }

    public static ChaosState killed(String targetNodeId, Instant nextEligibleAt, Instant nextAutoAt){
        return new ChaosState(ChaosPhase.KILLED, nextEligibleAt, nextAutoAt, targetNodeId, null);
    }
}