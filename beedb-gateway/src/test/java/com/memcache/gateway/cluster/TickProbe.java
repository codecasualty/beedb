package com.memcache.gateway.cluster;
import java.time.Clock;
import java.util.*;

import com.memcache.gateway.chaos.ChaosService;
import com.memcache.gateway.chaos.FakeNodeSupervisor;
import com.memcache.gateway.client.BeedbClient;
import com.memcache.gateway.demo.DemoWriter;
public class TickProbe {
    public static void main(String[] a) throws Exception {
        Map<String,String> nodes = new LinkedHashMap<>();
        nodes.put("node1","localhost:11211");
        nodes.put("node2","localhost:11212");
        nodes.put("node3","localhost:11213");
        ClusterStatusService svc = new ClusterStatusService(
            new BeedbClient(nodes, 86400), 
            new ChaosService(new FakeNodeSupervisor(), 10, 240, 30, Clock.systemUTC(), 10, Runnable::run),
        new DemoWriter(new BeedbClient(nodes, 86400), null));
        for (int i = 1; i <= 3; i++) {
            try {
                svc.updateClusterStatus();
                System.out.println("tick " + i + " ok  -> " + svc.getClusterSnapshot().snapshotTime());
            } catch (Throwable t) {
                System.out.println("tick " + i + " THREW " + t);
            }
            Thread.sleep(1000);
        }
    }
}
