package com.memcache.gateway.client;

import java.util.Map;

import com.memcache.gateway.kv.KeyRegistry;
import com.memcache.gateway.kv.WriteRateLimiter;

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

        public int getKeyTtlSeconds() { return keyTtlSeconds; }
        public void setKeyTtlSeconds(int keyTtlSeconds) { this.keyTtlSeconds = keyTtlSeconds; }
        public int getMaxTrackedKeys() { return maxTrackedKeys; }
        public void setMaxTrackedKeys(int maxTrackedKeys) { this.maxTrackedKeys = maxTrackedKeys; }
        public int getMaxWritesPerHour() { return maxWritesPerHour; }
        public void setMaxWritesPerHour(int maxWritesPerHour) { this.maxWritesPerHour = maxWritesPerHour; }
        public int getMaxValueBytes() { return maxValueBytes; }
        public void setMaxValueBytes(int maxValueBytes) { this.maxValueBytes = maxValueBytes; }

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
}
