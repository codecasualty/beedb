package com.memcache.gateway.chaos;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import com.memcache.gateway.cluster.ClusterSnapshot;
import com.memcache.gateway.cluster.NodeStatus;

/**
 * Builds the ClusterSnapshots ChaosService reasons about, without a cluster.
 */
public final class Snapshots {

    private Snapshots() { }

    public static final String NODE1 = "node1";
    public static final String NODE2 = "node2";
    public static final String NODE3 = "node3";
    private static final List<String> ALL = List.of(NODE1, NODE2, NODE3);

    /** all three reachable, `leaderId` leads, quorum true. The only shape healthyCluster() accepts. */
    public static ClusterSnapshot healthy(String leaderId) {
        return build(ALL, leaderId, leaderId, true, List.of());
    }

    /**
     * `downNodeId` unreachable, the other two reachable and following `leaderId`.
     * Quorum still true -- 2 of 3. This is what the cluster looks like between the
     * kill landing and the node coming back.
     */
    public static ClusterSnapshot withNodeDown(String downNodeId, String leaderId) {
        return build(ALL, leaderId, leaderId, true, List.of(downNodeId));
    }

    /**
     * all three reachable but nobody reports LEADER -- a snapshot taken mid-election.
     * leaderId is null, exactly as ClusterStatusService computes it when no node says
     * LEADER, and raftLeaderId is null on every node too (the server omits the field).
     */
    public static ClusterSnapshot noLeader() {
        List<NodeStatus> nodes = new ArrayList<>();
        for (String id : ALL) {
            nodes.add(NodeStatus.of(0, id, "FOLLOWER", 7L, 10, 10, null, 0, 10));
        }
        return new ClusterSnapshot(nodes, null, true, Instant.parse("2026-01-01T00:00:00Z"));
    }

    /** two of three unreachable: the survivor still claims LEADER but quorum is false. */
    public static ClusterSnapshot quorumLost(String survivingLeaderId) {
        List<String> down = new ArrayList<>(ALL);
        down.remove(survivingLeaderId);
        return build(ALL, survivingLeaderId, survivingLeaderId, false, down);
    }

    /**
     * the empty snapshot ClusterStatusService lazily creates before its first poll
     * completes. A real tick can see this, so the health gate has to survive it --
     * note that "every node's role is non-null" is VACUOUSLY TRUE on an empty list.
     */
    public static ClusterSnapshot empty() {
        return new ClusterSnapshot(new ArrayList<>(), null, false, Instant.parse("2026-01-01T00:00:00Z"));
    }


    private static ClusterSnapshot build(List<String> allNodes, String leaderId,
                                         String reportedLeaderId, boolean quorum,
                                         List<String> downNodeIds) {
        List<NodeStatus> nodes = new ArrayList<>();
        for (String id : allNodes) {
            if (downNodeIds.contains(id)) {
                nodes.add(NodeStatus.unreachable(id, id + ":11211"));
            } else {
                String role = id.equals(leaderId) ? "LEADER" : "FOLLOWER";
                nodes.add(NodeStatus.of(0, id, role, 7L, 10, 10, reportedLeaderId, 0, 10));
            }
        }
        return new ClusterSnapshot(nodes, leaderId, quorum, Instant.parse("2026-01-01T00:00:00Z"));
    }
}
