package com.memcache.gateway.cluster;

import java.time.Instant;
import java.util.List;
/*
* A snapshot of the cluster at a given point in time , which 
* is used to determine the current cluster state and report it to 
* the client (browser)
*/
public record ClusterSnapshot(
    List<NodeStatus> nodes, 
    String leaderId, 
    boolean quorum, 
    Instant snapshotTime
) {}