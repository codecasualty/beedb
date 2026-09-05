package com.memcache.gateway.cluster;
import java.util.*;
import com.memcache.gateway.client.BeedbClient;
public class TickProbe {
    public static void main(String[] a) throws Exception {
        Map<String,String> nodes = new LinkedHashMap<>();
        nodes.put("node1","localhost:11211");
        nodes.put("node2","localhost:11212");
        nodes.put("node3","localhost:11213");
        ClusterStatusService svc = new ClusterStatusService(new BeedbClient(nodes, 86400));
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
