package com.memcache.gateway.supervisor;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.gateway.chaos.chaosexception.NodeCouldNotBeKilled;

/**
 * when we have nodes on real server there we need systemd to stop and start them
 * we can't user {@link ProcessNodeSupervisor} because pkill sends SIGTERM, the
 * JVM exits 143, and the unit declares 143 a clean stop , so systemd would
 * consider the node deliberately stopped, while this process later started a
 * replacement outside systemd, in the wrong working directory, that systemd
 * knows nothing about. 
 *
 * going through systemctl keeps one owner of the process. `stop` is an explicit
 * request, so `Restart=on-failure` does not fire, the node stays down until we
 * start it again, which is exactly what a chaos demo wants. 
 */
public class SystemdNodeSupervisor implements NodeSupervisor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SystemdNodeSupervisor.class);
    private static final int SYSTEMCTL_TIMEOUT_SECONDS = 10;

    private final Set<String> nodeIds;
    private final String unitPattern;
    private final int restartAfterSeconds;

    private final ThreadFactory chaosRestoreThreadFactory = runnable -> {
        Thread thread = new Thread(runnable, "chaos-restore");
        thread.setDaemon(true);
        return thread;
    };
    private final ScheduledExecutorService chaosRestorer =
        Executors.newScheduledThreadPool(1, chaosRestoreThreadFactory);

    public SystemdNodeSupervisor(Set<String> nodeIds, String unitPattern, int restartAfterSeconds) {
        this.nodeIds = nodeIds;
        this.unitPattern = unitPattern;
        this.restartAfterSeconds = restartAfterSeconds;
    }

    @Override
    public void kill(String nodeId) throws NodeCouldNotBeKilled {
        // the node id becomes part of a command run as root, so it is checked
        // against the configured set and never taken on trust. The sudoers rule
        // names each unit in full as a second line of defence.
        if (nodeId == null || !nodeIds.contains(nodeId)) {
            throw new NodeCouldNotBeKilled("node " + nodeId + " is not in the set of nodes to kill");
        }
        systemctl("stop", nodeId);
        LOGGER.info("chaos stopped {}, restoring in {}s", nodeId, restartAfterSeconds);

        // the timer lives in this JVM: if the gateway restarts inside the
        // window, the node stays stopped until someone starts it
        chaosRestorer.schedule(() -> {
            try {
                systemctl("start", nodeId);
                LOGGER.info("chaos restored {}", nodeId);
            } catch (Exception e) {
                LOGGER.error("failed to restore node {}", nodeId, e);
            }
        }, restartAfterSeconds, TimeUnit.SECONDS);
    }

    private void systemctl(String verb, String nodeId) throws NodeCouldNotBeKilled {
        String unit = String.format(unitPattern, nodeId);
        List<String> command = List.of("sudo", "-n", "systemctl", verb, unit);
        try {
            Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
            if (!process.waitFor(SYSTEMCTL_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new NodeCouldNotBeKilled("systemctl " + verb + " " + unit + " timed out");
            }
            int exitCode = process.exitValue();
            if (exitCode != 0) {
                // the sudoers rule is missing, or it does not
                // cover this unit name. Both are silent until a kill is tried.
                throw new NodeCouldNotBeKilled("systemctl " + verb + " " + unit
                    + " exited " + exitCode + ": " + new String(process.getInputStream().readAllBytes()));
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NodeCouldNotBeKilled("systemctl " + verb + " " + unit + " was interrupted", e);
        } catch (IOException e) {
            throw new NodeCouldNotBeKilled("systemctl " + verb + " " + unit + " failed: " + e.getMessage(), e);
        }
    }
}
