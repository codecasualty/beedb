package com.memcache.gateway.client;
import java.util.*;
public class ExProbe {
    public static void main(String[] a) throws Exception {
        Map<String,String> nodes = new LinkedHashMap<>();
        nodes.put("node1","127.0.0.1:11211");
        nodes.put("node2","127.0.0.1:11212");
        nodes.put("node3","127.0.0.1:11213");
        BeedbClient c = new BeedbClient(nodes, 86400);
        System.out.println("leader = " + c.getleaderAddressString());
        System.out.println("warmup set -> " + c.set("ex","v"));
        System.out.println(">>> now kill that node; retrying every 2s for 40s");
        for (int i=0;i<20;i++){
            try { c.get("ex"); }
            catch (Exception e) {
                System.out.println("get -> " + e.getClass().getSimpleName() + " : " + e.getMessage());
            }
            try { c.set("ex","v2"); }
            catch (Exception e) {
                System.out.println("set -> " + e.getClass().getSimpleName() + " : " + e.getMessage());
            }
            Thread.sleep(2000);
        }
    }
}
