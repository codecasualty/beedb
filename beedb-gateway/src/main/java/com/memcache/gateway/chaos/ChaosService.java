package com.memcache.gateway.chaos;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.memcache.gateway.supervisor.NodeSupervisor;

public class ChaosService {
    private final AtomicReference<ChaosState> chaosState = new AtomicReference<>();
    private final NodeSupervisor nodeSupervisor;
    private final Clock clock;
    private final int announceLeadSeconds;
    private final int autoKillSeconds;
    private final int cooldownSeconds;
    private static final int STARTING_SECONDS = 60;
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

    public ChaosState getChaosState(){
        return chaosState.get();
    }
}
