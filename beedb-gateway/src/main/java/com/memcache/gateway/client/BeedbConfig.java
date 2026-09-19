package com.memcache.gateway.client;

import java.nio.file.Path;
import java.time.Clock;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.memcache.gateway.chaos.ChaosService;
import com.memcache.gateway.kv.KeyRegistry;
import com.memcache.gateway.kv.WriteRateLimiter;
import com.memcache.gateway.supervisor.NodeSupervisor;
import com.memcache.gateway.supervisor.ProcessNodeSupervisor;
import com.memcache.gateway.supervisor.SystemdNodeSupervisor;

import ch.qos.logback.core.pattern.parser.Node;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * creates the single BeedbClient the whole gateway shares.
 *
 * one instance, not one per request: BeedbClient owns the connection pool and the
 * cached leader address. A new one per request would open fresh sockets and
 * re-resolve the leader every time.
 */
@Configuration
@EnableConfigurationProperties(BeedbConfig.BeedbProperties.class)
public class BeedbConfig {

    /**
     * holds beedb.nodes.* from application.properties.
     *
     * prefix "beedb" + field name "nodes" = the key beedb.nodes.*, so this map
     * arrives as { node1 -> localhost:11211, node2 -> ..., node3 -> ... }.
     *
     * map KEY must match the nodeId the server reports in `stats` as
     * raft_leader_id -- that string is how a "Not Leader: node2" redirect gets
     * turned back into an address.
     */
    @ConfigurationProperties(prefix = "beedb")
    public static class BeedbProperties {
        private Map<String, String> nodes;

        /**
         * Seconds a written key lives. Bound to beedb.key-ttl-seconds -- Spring's
         * relaxed binding maps the kebab-case property onto this camelCase field.
         */
        private int keyTtlSeconds = 86400;

        /** Cap on how many keys the registry tracks. A public demo gets poked at. */
        private int maxTrackedKeys = 500;

        /** Writes allowed per client address per hour. */
        private int maxWritesPerHour = 10;

        /** Largest value a client may store, bytes. See application.properties. */
        private int maxValueBytes = 8192;

        /** a node will be killed after these many seconds of receiving kill command from client */
        private int announceLeadSeconds = 10;

        /** waiting time before triggering next kill command on leader */
        private int autoKillSeconds = 240;

        /** if we dont receive kill command, automatically trigger leader kill after these many seconds */
        private int cooldownSeconds = 30;

        private int chaosStartupGraceSeconds = 60;

        private String supervisorServerDir = "../beedb-server";

        private int restartAfterSeconds = 15;

        /** "process" on a laptop (nodes started by make), "systemd" on a server. */
        private String supervisor = "process";

        /** Only read when supervisor=systemd. %s is the node id. */
        private String systemdUnitPattern = "beedb-node@%s.service";


        public int getKeyTtlSeconds() { return keyTtlSeconds; }
        public void setKeyTtlSeconds(int keyTtlSeconds) { this.keyTtlSeconds = keyTtlSeconds; }
        public int getMaxTrackedKeys() { return maxTrackedKeys; }
        public void setMaxTrackedKeys(int maxTrackedKeys) { this.maxTrackedKeys = maxTrackedKeys; }
        public int getMaxWritesPerHour() { return maxWritesPerHour; }
        public void setMaxWritesPerHour(int maxWritesPerHour) { this.maxWritesPerHour = maxWritesPerHour; }
        public int getMaxValueBytes() { return maxValueBytes; }
        public void setMaxValueBytes(int maxValueBytes) { this.maxValueBytes = maxValueBytes; }
        public int getAnnounceLeadSeconds() { return announceLeadSeconds; }
        public void setAnnounceLeadSeconds(int announceLeadSeconds) { this.announceLeadSeconds = announceLeadSeconds; }
        public int getAutoKillSeconds() { return autoKillSeconds; }
        public void setAutoKillSeconds(int autoKillSeconds) { this.autoKillSeconds = autoKillSeconds; }
        public int getCooldownSeconds() { return cooldownSeconds; }
        public void setCooldownSeconds(int cooldownSeconds) { this.cooldownSeconds = cooldownSeconds; }
        public int getChaosStartupGraceSeconds() { return chaosStartupGraceSeconds; }
        public void setChaosStartupGraceSeconds(int chaosStartupGraceSeconds) { this.chaosStartupGraceSeconds = chaosStartupGraceSeconds; }
        public String getSupervisorServerDir() { return supervisorServerDir; }
        public void setSupervisorServerDir(String supervisorServerDir) { this.supervisorServerDir = supervisorServerDir; }
        public int getRestartAfterSeconds() { return restartAfterSeconds; }
        public void setRestartAfterSeconds(int restartAfterSeconds) { this.restartAfterSeconds = restartAfterSeconds; }
        public String getSupervisor() { return supervisor; }
        public void setSupervisor(String supervisor) { this.supervisor = supervisor; }
        public String getSystemdUnitPattern() { return systemdUnitPattern; }
        public void setSystemdUnitPattern(String systemdUnitPattern) { this.systemdUnitPattern = systemdUnitPattern; }

        public Map<String, String> getNodes() {
            return nodes;
        }

        public void setNodes(Map<String, String> nodes) {
            this.nodes = nodes;
        }
    }

    private final BeedbProperties beedbProperties;

    public BeedbConfig(BeedbProperties beedbProperties) {
        this.beedbProperties = beedbProperties;
    }

    /**
     * The registry expires entries on the SAME ttl the cluster expires the data on.
     * Two clocks that could drift apart would leave keys whose owner had expired but
     * whose data had not -- writable by anyone, for reasons invisible from the UI.
     */
    @Bean
    public KeyRegistry keyRegistry() {
        return new KeyRegistry(beedbProperties.getKeyTtlSeconds(),
                               beedbProperties.getMaxTrackedKeys());
    }

    @Bean
    public WriteRateLimiter writeRateLimiter() {
        return new WriteRateLimiter(beedbProperties.getMaxWritesPerHour());
    }

    @Bean(destroyMethod = "close")
    public BeedbClient beedbClient() {
        Map<String, String> nodes = beedbProperties.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalStateException(
                    "no beedb.nodes.* entries found in application.properties");
        }
        System.out.println("nodes " + nodes);
        return new BeedbClient(nodes, beedbProperties.getKeyTtlSeconds());
    }

    @Bean 
    public Clock clock(){
        return Clock.systemUTC();
    }

    @Bean 
    public NodeSupervisor nodeSupervisor(){
        Set<String> nodeIds = beedbProperties.getNodes().keySet();
        String supervisorServerDir = beedbProperties.getSupervisorServerDir();
        int restartAfterSeconds = beedbProperties.getRestartAfterSeconds();
        String supervisor = beedbProperties.getSupervisor();
        if ("systemd".equalsIgnoreCase(supervisor)) {
            return new SystemdNodeSupervisor(Set.copyOf(nodeIds),
                beedbProperties.getSystemdUnitPattern(), restartAfterSeconds);
        }
        if (!"process".equalsIgnoreCase(supervisor)) {
            throw new IllegalStateException("beedb.supervisor must be 'process' or 'systemd', got: " + supervisor);
        }
        return new ProcessNodeSupervisor(Set.copyOf(nodeIds), Path.of(supervisorServerDir), restartAfterSeconds);
    }

    @Bean 
    public ChaosService chaosService(NodeSupervisor nodeSupervisor, Clock clock){
        int announceLeadSeconds = beedbProperties.getAnnounceLeadSeconds();
        int autoKillSeconds = beedbProperties.getAutoKillSeconds();
        int cooldownSeconds = beedbProperties.getCooldownSeconds();
        int startingSeconds = beedbProperties.getChaosStartupGraceSeconds();
        Executor killExecutor = Executors.newFixedThreadPool(1 , killThreadFactory());
        return new ChaosService(nodeSupervisor, announceLeadSeconds, autoKillSeconds, cooldownSeconds, clock, startingSeconds, killExecutor);
    }

    private ThreadFactory killThreadFactory(){
        ThreadFactory threadFactory = runnable -> {
            Thread thread = new Thread(runnable, "chaos-kill");
            thread.setDaemon(true);
            return thread;
        };
        return threadFactory;
    }
}
