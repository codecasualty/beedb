package com.memcache.gateway.supervisor;

import java.nio.file.Path;
import java.util.Set;

public class SupervisorProbe {
    public static void main(String[] args) throws Exception {
        ProcessNodeSupervisor s = new ProcessNodeSupervisor(
                Set.of("node1", "node2", "node3"), Path.of("../beedb-server"), 15);
        s.kill("node2");
        System.out.println("killed node2, waiting for restore...");
        Thread.sleep(25_000);        // restore thread is a daemon
    }
}
