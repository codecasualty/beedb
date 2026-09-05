package com.memcache.gateway.client;
import java.util.*;
import com.memcache.gateway.cluster.NodeStatus;
public class StatProbe {
    public static void main(String[] a) {
        Map<String,String> nodes = new LinkedHashMap<>();
        nodes.put("node1","127.0.0.1:11211");
        nodes.put("node2","127.0.0.1:11212");
        nodes.put("node3","127.0.0.1:11213");
        BeedbClient c = new BeedbClient(nodes, 86400);
        for (Map.Entry<String,String> e : nodes.entrySet()) {
            NodeStatus s = c.getStats(e.getKey(), e.getValue());
            System.out.println(e.getKey() + " -> " + s);
        }
        System.out.println("\n-- a node that does not exist --");
        System.out.println("dead -> " + c.getStats("node9", "127.0.0.1:19999"));
        System.out.println("\n-- leader resolution --");
        try { System.out.println("leader -> " + c.getleaderAddressString()); }
        catch (Throwable t) { System.out.println("THREW " + t); }
    }
}
