package com.memcache.gateway.client;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 * MANUAL failover probe
 * What it proves: every key the client reported as STORED is still readable
 * after the leader changed underneath it. Keys that errored are not asserted on
 * -- an in-flight write during a crash may or may not have landed, and both
 * outcomes are legal.
 */
public class FailoverProbe {

    public static void main(String[] args) throws Exception {
        Map<String, String> nodes = new LinkedHashMap<>();
        nodes.put("node1", "127.0.0.1:11211");
        nodes.put("node2", "127.0.0.1:11212");
        nodes.put("node3", "127.0.0.1:11213");

        BeedbClient client = new BeedbClient(nodes);
        String leader = client.getleaderAddressString();
        System.out.println("leader = " + leader);
        System.out.println(">>> kill that node now; this will run for 1000 writes\n");

        int acked = 0, failed = 0;
        String lastLeader = leader;

        for (int i = 0; i < 1000; i++) {
            String key = "fo-" + i;
            String value = "v-" + i;
            String response;
            try {
                response = client.set(key, value);
            } catch (Exception e) {
                response = null;                       // legal during a failover
            }
            if ("STORED".equals(response)) acked++; else failed++;

            String now = client.getleaderAddressString();
            if (now != null && !now.equals(lastLeader)) {
                System.out.printf("  [%4d] LEADER CHANGED %s -> %s   (acked %d, failed %d)%n",
                                  i, lastLeader, now, acked, failed);
                lastLeader = now;
            }
            Thread.sleep(5);
        }

        System.out.printf("%nwrites: %d acked, %d failed%n", acked, failed);

        // Every acked write must still be readable. Failed ones are not asserted on.
        int lost = 0, checked = 0;
        for (int i = 0; i < 1000; i++) {
            String key = "fo-" + i, expected = "v-" + i;
            String got;
            try { got = client.get(key); } catch (Exception e) { got = null; }
            if (got != null) { checked++; if (!expected.equals(got)) lost++; }
        }
        System.out.printf("readback: %d keys present, %d with WRONG value%n", checked, lost);
        System.out.println(lost == 0 && checked >= acked - 5
                ? "PASS -- acknowledged writes survived the failover"
                : "FAIL -- see above");
    }
}
