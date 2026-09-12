package com.memcache.gateway.chaos;

import java.time.Instant;

// this will be shared with the client(on frontend)
public record ChaosView(
    ChaosPhase phase,
    String targetNodeId,
    Instant nextAutoAt,
    Long secondsUntilKill
) {}
