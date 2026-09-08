package com.memcache.gateway.supervisor;

import com.memcache.gateway.chaos.chaosexception.ChaosException;

public interface NodeSupervisor {
    void kill(String nodeId) throws ChaosException;
}
