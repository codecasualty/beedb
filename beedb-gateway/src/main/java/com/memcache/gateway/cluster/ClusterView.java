package com.memcache.gateway.cluster;

import com.memcache.gateway.chaos.ChaosView;
import com.memcache.gateway.demo.DemoRecord;

public record ClusterView (
    ClusterSnapshot clusterSnapshot,
    ChaosView chaosView,
    DemoRecord demoRecord
){}
