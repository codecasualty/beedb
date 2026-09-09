package com.memcache.gateway.chaos;

import java.util.Collections;
import java.util.List;
import java.util.ArrayList;

import com.memcache.gateway.chaos.chaosexception.ChaosException;
import com.memcache.gateway.chaos.chaosexception.NodeCouldNotBeKilled;
import com.memcache.gateway.supervisor.NodeSupervisor;

/**
 * Records what it was asked to kill instead of killing anything.
 *
 * two things this lets a test prove that a real supervisor cannot:
 *
 *   - the kill happened EXACTLY once, and on the node the banner named. A test that
 *     only checks the phase reached KILLED would pass even if the wrong node were
 *     targeted, or the same node killed on every tick.
 *   - what the state machine does when the kill FAILS -- `failEveryKill()` makes that
 *     a one-line arrangement instead of an unreproducible production incident.
 */
public final class FakeNodeSupervisor implements NodeSupervisor {

    private final List<String> killed = Collections.synchronizedList(new ArrayList<>());
    private volatile boolean shouldThrow = false;

    /** Every subsequent kill() records the attempt and then throws. */
    public FakeNodeSupervisor failEveryKill() {
        this.shouldThrow = true;
        return this;
    }

    public FakeNodeSupervisor succeedFromNowOn() {
        this.shouldThrow = false;
        return this;
    }

    /** Every node id kill() was called with, in order, successes and failures alike. */
    public List<String> killAttempts() {
        return new ArrayList<>(killed);
    }

    public int killCount() {
        return killed.size();
    }

    public String lastKilled() {
        List<String> snapshot = killAttempts();
        return snapshot.isEmpty() ? null : snapshot.get(snapshot.size() - 1);
    }

    @Override
    public void kill(String nodeId) throws ChaosException {
        killed.add(nodeId);
        if (shouldThrow) {
            throw new NodeCouldNotBeKilled("FakeNodeSupervisor refusing to kill " + nodeId);
        }
    }
}
