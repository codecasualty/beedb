package com.memcache.gateway.chaos;

public enum ChaosKillState {
    SUCCESS,
    CLUSTER_DEGRADED,
    KILLING_IN_PROGRESS,
    FAILED,
    UNDER_COOLDOWN,
    NOT_CORRECT_NODE_ID,
    KILLING_TIME_ANNOUNCED,
    ALREADY_KILLED
}
