package com.memcache.gateway.chaos;

public enum ChaosKillState {
    SUCCESS,
    CLUSTER_DEGRADED,
    FAILED,
    UNDER_COOLDOWN,
    KILLING_TIME_ANNOUNCED,
    ALREADY_KILLED
}
