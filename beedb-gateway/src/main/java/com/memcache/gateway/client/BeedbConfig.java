package com.memcache.gateway.client;

import java.util.Map;

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

    @Bean(destroyMethod = "close")
    public BeedbClient beedbClient() {
        Map<String, String> nodes = beedbProperties.getNodes();
        if (nodes == null || nodes.isEmpty()) {
            throw new IllegalStateException(
                    "no beedb.nodes.* entries found in application.properties");
        }
        System.out.println("nodes " + nodes);
        return new BeedbClient(nodes);
    }
}
