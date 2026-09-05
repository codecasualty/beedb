package com.memcache.gateway.kv;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

/**
 * Remembers which keys the gateway has written, and who wrote them.
 *
 * WHY THIS EXISTS AT ALL
 *
 * BeeDB speaks memcached, whose command set is SET / ADD / REPLACE / APPEND /
 * PREPEND / GET / DELETE / STATS. There is no `keys` and no `scan` -- you can only
 * ask about a key you already know the name of. The demo's store panel shows a list
 * of what is in the cluster, so somebody has to keep that list, and the gateway is
 * the only place that sees every write.
 *
 * WHY CAFFEINE RATHER THAN A ConcurrentHashMap
 *
 * entries must expire. Keys in the cluster disappear on their own after the TTL, and a plain 
 * map would keep claiming they exist forever -- the store panel would fill with ghosts. Caffeine
 * evicts on the same clock. It is also safe for the request threads and the status
 * poller to touch concurrently.
 *
 * WHAT THIS IS NOT
 *
 * not authorization. A session id arrives in a request header and can be forged with
 * one line in devtools. It stops two visitors clobbering each other by accident; it
 * would stop nobody who was trying.
 *
 * not durable. It lives in this process's heap. while the data itself is still
 * sitting in the cluster.
 */
public class KeyRegistry {

    /** One row of the store panel. */
    public record Entry(String key, String sessionId, Instant writtenAt, int version) {}

    private final Cache<String, Entry> entries;

    public KeyRegistry(int ttlSeconds, int maxKeys) {
        this.entries = Caffeine.newBuilder()
                // Same clock as the cluster's own expiry, deliberately. Two TTLs that
                // could disagree would produce keys whose owner had expired but whose
                // data had not -- editable by anyone, for reasons invisible from here.
                .expireAfterWrite(Duration.ofSeconds(ttlSeconds))
                // A public demo will be poked at. Without a bound this is a memory
                // leak with a stranger's finger on the pump.
                .maximumSize(maxKeys)
                .build();
    }

    /**
     * Who owns this key, or null if nobody does -- either it was never written
     * through this gateway, or its entry has expired.
     *
     * A null owner means "anyone may write it". That is deliberate: after a gateway
     * restart every key is unowned, and the alternative (nobody may write anything)
     * would be a worse failure.
     */
    public String ownerOf(String key) {
        Entry entry = entries.getIfPresent(key);
        return entry == null ? null : entry.sessionId();
    }

    /** True if this session may write this key: either nobody owns it, or they do. */
    public boolean mayWrite(String key, String sessionId) {
        String owner = ownerOf(key);
        return owner == null || owner.equals(sessionId);
    }

    /**
     * Record a successful write. Re-writing your own key bumps the version and
     * restarts the TTL, matching what the cluster does to the data.
     */
    public void recordWrite(String key, String sessionId) {
        Entry existing = entries.getIfPresent(key);
        int version = existing == null ? 1 : existing.version() + 1;
        entries.put(key, new Entry(key, sessionId, Instant.now(), version));
    }

    public void recordDelete(String key) {
        entries.invalidate(key);
    }

    /** Newest first, which is the order the store panel wants to render. */
    public List<Entry> list() {
        List<Entry> all = new ArrayList<>(entries.asMap().values());
        all.sort(Comparator.comparing(Entry::writtenAt).reversed());
        return all;
    }

    public Map<String, Entry> asMap() {
        return entries.asMap();
    }
}
