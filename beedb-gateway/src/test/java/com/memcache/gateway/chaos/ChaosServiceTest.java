package com.memcache.gateway.chaos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.gateway.cluster.ClusterSnapshot;

/**
 * the state machine, proven without a cluster, without Spring, without sockets.
 *
 * everything ChaosService needs is injected: the snapshot is a parameter, the clock is
 * a field, the supervisor is an interface. So each test is arrange-a-world / call
 * tick() or requestKill() / assert no waiting, no flakiness, no ports.
 *
 * the startup grace matters for arranging: the constructor sets
 * nextEligibleAt = now + 60s and nextAutoAt = now + autoKillSeconds so the auto path
 * cannot fire until BOTH have passed with the values below, 240s.
 */
public class ChaosServiceTest {

    private static final int ANNOUNCE_LEAD_SECONDS = 10;
    private static final int AUTO_KILL_SECONDS     = 240;
    private static final int COOLDOWN_SECONDS      = 30;
    private static final int STARTING_SECONDS      = 5;

    private final MutableClock        clock      = MutableClock.atEpochStart();
    private final FakeNodeSupervisor  supervisor = new FakeNodeSupervisor();
    private final ChaosService        chaos      = new ChaosService(
            supervisor, ANNOUNCE_LEAD_SECONDS, AUTO_KILL_SECONDS, COOLDOWN_SECONDS, clock, STARTING_SECONDS);
    private static final Logger LOGGER = LoggerFactory.getLogger(ChaosServiceTest.class);


    public ClusterSnapshot reachIdleState(){
        ClusterSnapshot snapshot = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
        return snapshot;
    }

    public ClusterSnapshot reachAnnouncedState(){
        ClusterSnapshot snapshot = reachIdleState();
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.ANNOUNCED , chaos.getState().phase());
        return snapshot;
    }

    public ClusterSnapshot reachKilledState(){
        ClusterSnapshot snapshot = reachAnnouncedState();
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.KILLED , chaos.getState().phase());
        return snapshot;
    }

    @Test
    public void idleStaysIdleDuringStartupGrace() {
        ClusterSnapshot snapshot = Snapshots.healthy("node1");
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
        clock.advanceSeconds(STARTING_SECONDS);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
    }

    @Test
    public void idleAnnouncesOnceBothClocksElapse() {
        /*
        once both clocks have passed, tick will advance from idle to annoucned
         */
        ClusterSnapshot snapshot  = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(snapshot);
        Instant now = clock.instant();
        assertEquals(ChaosPhase.ANNOUNCED , chaos.getState().phase());
        assertEquals(now.plus(ANNOUNCE_LEAD_SECONDS, ChronoUnit.SECONDS), chaos.getState().killAt());
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.KILLED , chaos.getState().phase());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(null, chaos.getState().killAt());
    }

    @Test
    public void announcedDoesNotKillBeforeKillAt() {
        /*
        announced phase does not kill before killAt
         */
        ClusterSnapshot snapshot  = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.ANNOUNCED , chaos.getState().phase());
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS - 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.ANNOUNCED , chaos.getState().phase());
        assertEquals(0, supervisor.killCount());
    }

    @Test
    public void announcedKillsExactlyOnceAtKillAt() {
        /*
        announced phase kills exactly once at killAt
         */
        ClusterSnapshot snapshot  = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.IDLE , chaos.getState().phase());
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.ANNOUNCED , chaos.getState().phase());
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        chaos.tick(snapshot);
        assertEquals(ChaosPhase.KILLED , chaos.getState().phase());
        assertEquals(1, supervisor.killCount());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        chaos.tick(snapshot);
        assertEquals(1, supervisor.killCount());
    }

    @Test
    public void killedDoesNotRecoverWhileTargetStillAnswers() {
        /*
        * in killed phase, we should not change the phase , node wont be forever in kill state because
        * in prod killed will be done by supervisor and alwasy restart will bring back the node
         */
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(STARTING_SECONDS + 1);
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(false, chaos.getState().observedDown());
        assertNotNull(chaos.getState().targetNodeId());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(1, supervisor.killCount());
        chaos.tick(clusterSnapshot);
        assertEquals(1, supervisor.killCount());
    }

    @Test
    public void killedRecoversOnlyAfterSeenDownThenBack() {
        /*
        * in killed phase, we should not change the phase untill and unless we are sure the node 
        * has died and recovered
         */
        ClusterSnapshot clusterSnapshot = reachKilledState();
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(false, chaos.getState().observedDown());
        clusterSnapshot = Snapshots.withNodeDown("node1", "node2");
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(5);
        assertEquals(true, chaos.getState().observedDown());
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        clusterSnapshot = Snapshots.healthy("node2");
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        Instant nextEligibleAt = clock.instant().plus(COOLDOWN_SECONDS, ChronoUnit.SECONDS);
        clock.advanceSeconds(COOLDOWN_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        assertEquals(nextEligibleAt, chaos.getState().nextEligibleAt());
    }

    @Test
    public void killedStaysKilledForeverIfTargetNeverReturns() {
        /*
        * in killed phase, no matter how many time we call kill our state should not change
         */
        ClusterSnapshot clusterSnapshot = reachKilledState();
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(false, chaos.getState().observedDown());
        clusterSnapshot = Snapshots.withNodeDown("node1", "node2");
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(5);
        assertEquals(true, chaos.getState().observedDown());
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(1, supervisor.killCount());
        clock.advanceSeconds(10);
        chaos.tick(clusterSnapshot);
        assertEquals(1, supervisor.killCount());
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        for(int i = 0;i < 100;i++){
            clock.advanceSeconds(10);
            chaos.tick(clusterSnapshot);
            assertEquals(1, supervisor.killCount());
            assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        }
    }


    @Test
    public void doesNotAnnounceWhenNoLeader() { 
        /* Snapshots.noLeader()   -> stays IDLE */ 
        ClusterSnapshot clusterSnapshot = Snapshots.noLeader();
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(STARTING_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(AUTO_KILL_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
    }

    @Test
    public void doesNotAnnounceWhenQuorumLost(){ 
        /* Snapshots.quorumLost() -> stays IDLE */ 

        ClusterSnapshot clusterSnapshot = Snapshots.quorumLost("node1");
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(STARTING_SECONDS);
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(AUTO_KILL_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
    }

    @Test
    public void doesNotAnnounceWhenANodeAlreadyDown() {
        // withNodeDown(...) -> stays IDL chaos must never pile onto real damage.
        ClusterSnapshot clusterSnapshot = Snapshots.withNodeDown("node1", "node2");
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(STARTING_SECONDS);
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(AUTO_KILL_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());

    }

    @Test
    public void doesNotAnnounceOnTheEmptyPrePollSnapshot() {
        // Snapshots.empty() -> stays IDLE
        ClusterSnapshot clusterSnapshot = Snapshots.empty();
        clock.advanceSeconds(STARTING_SECONDS);
        clock.advanceSeconds(AUTO_KILL_SECONDS);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        assertEquals(0, supervisor.killCount());

    }


    @Test
    public void buttonWorksDuringTheAutoGap() {
        /*
        * during now >= eligibleAt button should work
         */
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.SUCCESS, state);

    }

    @Test
    public void buttonRefusedUnderCooldown() {
        // Straight after a completed round -> UNDER_COOLDOWN.
        ClusterSnapshot clusterSnapshot = reachKilledState();
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(false, chaos.getState().observedDown());
        clock.advanceSeconds(10);
        clusterSnapshot = Snapshots.withNodeDown("node1", "node2");
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
        assertEquals(true, chaos.getState().observedDown());
        clock.advanceSeconds(10);
        clusterSnapshot = Snapshots.healthy("node2");
        chaos.tick(clusterSnapshot);
        ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.UNDER_COOLDOWN, state);
    }

    @Test
    public void buttonRefusedWhileAnnounced(){ 
        /* -> KILLING_TIME_ANNOUNCED */ 
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.SUCCESS, state);
        state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.KILLING_TIME_ANNOUNCED, state);
    }

    @Test
    public void buttonRefusedWhileKilled(){ 
        /* -> ALREADY_KILLED */ 
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.SUCCESS, state);
        // at this point we have node that needs to be killed after current + announced seconds
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.KILLED, chaos.getState().phase());
        state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.ALREADY_KILLED, state);
        assertEquals(1, supervisor.killCount());
        assertEquals(Snapshots.NODE1, chaos.getState().targetNodeId());
    }

    @Test
    public void buttonRefusedWhenClusterDegraded(){ 
        /* -> CLUSTER_DEGRADED */ 
        ClusterSnapshot clusterSnapshot = Snapshots.quorumLost("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
        assertEquals(ChaosKillState.CLUSTER_DEGRADED, state);
    }

    @Test
    public void onlyOneOfManySimultaneousClicksWins() throws InterruptedException {
        /*
        * for n concurrent request only one should be able to announce the kill of nodes
         */
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        
        CountDownLatch ready = new CountDownLatch(10);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(10);
        AtomicInteger atomicInteger = new AtomicInteger(0);
        clock.advanceSeconds(STARTING_SECONDS + 1);
        for(int i = 0; i < 10; i++){
            new Thread(() -> {
                ready.countDown();
                LOGGER.info("thread "+Thread.currentThread().getName()+" started");
                try {
                    start.await();
                    ChaosKillState state = chaos.requestKill("node1", clusterSnapshot);
                    if(state == ChaosKillState.SUCCESS){
                        atomicInteger.getAndIncrement();
                    }
                }catch(InterruptedException e){
                    e.printStackTrace();
                }finally{
                    done.countDown();
                }
                LOGGER.info("thread "+Thread.currentThread().getName()+" finished state is ");
            }).start();
        }
        ready.await();
        start.countDown();
        done.await();
        // assertEquals(ChaosKillState.SUCCESS, chaos.requestKill("nodeId", clusterSnapshot));
        assertEquals(1, atomicInteger.get());

    }


    @Test
    public void supervisorFailureDoesNotWedgeTheMachine() {
        /*
        * for failure by supervisor we should be terminally in killed state, but revert back to idle state 
        */
        ClusterSnapshot clusterSnapshot = Snapshots.healthy("node1");
        clock.advanceSeconds(STARTING_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        clock.advanceSeconds(AUTO_KILL_SECONDS + 1);
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.ANNOUNCED, chaos.getState().phase());
        
        supervisor.failEveryKill();
        clock.advanceSeconds(ANNOUNCE_LEAD_SECONDS + 1);
        Instant now = clock.instant();
        chaos.tick(clusterSnapshot);
        assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
        assertNull(chaos.getState().targetNodeId());
        assertNull(chaos.getState().killAt());
        assertEquals(1, supervisor.killCount());
        assertEquals(now.plus(COOLDOWN_SECONDS, ChronoUnit.SECONDS), chaos.getState().nextEligibleAt());
        assertEquals(now.plus(AUTO_KILL_SECONDS, ChronoUnit.SECONDS), chaos.getState().nextAutoAt());

        for(int i = 0;i < 3;i++){
            clock.advanceSeconds(5);
            chaos.tick(clusterSnapshot);
            assertEquals(ChaosPhase.IDLE, chaos.getState().phase());
            assertEquals(null, chaos.getState().targetNodeId());
            assertEquals(null, chaos.getState().killAt());
            assertEquals(1, supervisor.killCount());
        }
        
    }
}
