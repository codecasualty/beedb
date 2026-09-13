package com.memcache.gateway.api;

import jakarta.servlet.http.HttpServletRequest;

import java.nio.charset.StandardCharsets;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.memcache.gateway.client.BeedbClient;
import com.memcache.gateway.client.BeedbConfig;
import com.memcache.gateway.demo.DemoWriter;
import com.memcache.gateway.kv.KeyRegistry;
import com.memcache.gateway.kv.WriteRateLimiter;

@RestController
@RequestMapping("/api/kv")
public class KvController {
    
    /**
     * Identifies the browser session. Client-generated, sent on every write.
     *
     * It stops two visitors clobbering each other by accident on a shared public demo
     */
    private static final String SESSION_HEADER = "X-Beedb-Session";

    private static final Logger LOGGER = LoggerFactory.getLogger(KvController.class);

    private final BeedbClient beedbClient;
    private final KeyRegistry keyRegistry;
    private final WriteRateLimiter rateLimiter;
    private final int maxValueBytes;

    public KvController(BeedbClient beedbClient, KeyRegistry keyRegistry,
                        WriteRateLimiter rateLimiter, BeedbConfig.BeedbProperties properties){
        this.beedbClient = beedbClient;
        this.keyRegistry = keyRegistry;
        this.rateLimiter = rateLimiter;
        this.maxValueBytes = properties.getMaxValueBytes();
    }

    /**
     * 429 when the client is out of writes for this hour.
     *
     * Note what is NOT used as the key: the session header. It is browser-generated,
     * so a client that is refused simply sends a new one. Only the source address is
     * something they cannot trivially change -- and behind a proxy even that needs
     * forward-headers configured before it means anything.
     */
    private ResponseEntity<String> rateLimited(HttpServletRequest request) {
        String client = request.getRemoteAddr();
        if (rateLimiter.tryAcquire(client)) {
            return null;
        }
        LOGGER.warn("rate limit hit by {} ({} writes/hour)", client, rateLimiter.maxWritesPerHour());
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                .header("Retry-After", "3600")
                .body("write limit reached: " + rateLimiter.maxWritesPerHour()
                      + " writes per hour. This is a shared demo.");
    }

    /**
     * 403 for keys the demo writer owns,checked before
     * the rate limit, so a refused request does not also cost the visitor a write.
     */
    private ResponseEntity<String> reservedKey(String key) {
        if (!key.startsWith(DemoWriter.KEY_PREFIX)) {
            return null;
        }
        LOGGER.debug("rejected write to reserved demo key {}", key);
        return ResponseEntity.status(HttpStatus.FORBIDDEN)
                .body("key '" + key + "' is reserved for the demo writer");
    }

    /**
     * Every key the gateway has written and who owns it.
     *
     * This endpoint exists because BeeDB cannot enumerate keys -- memcached has no
     * scan. Without the registry the store panel would have nothing to render but the
     * keys the current browser happened to write itself.
     */
    @GetMapping
    public ResponseEntity<List<KeyRegistry.Entry>> keys(
            @RequestParam(value = "limit", defaultValue = "100") int limit,
            @RequestHeader(value = SESSION_HEADER, required = false) String sessionId){
        List<KeyRegistry.Entry> all = keyRegistry.list();            // newest first
        int n = Math.max(0, Math.min(limit, 200));
        List<KeyRegistry.Entry> out = new ArrayList<>(all.subList(0, Math.min(n, all.size())));
        if(sessionId != null){
            for(KeyRegistry.Entry e : all){
                if(sessionId.equals(e.sessionId()) && !out.contains(e)) out.add(e);
            }
        }
        return ResponseEntity.ok(out);
    }

    @GetMapping ("/{key}")
    public ResponseEntity<String> get(@PathVariable("key") String key){
        String value = beedbClient.get(key);
        if(value == null){
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(value);
    }
    @PutMapping (value="/{key}", consumes = MediaType.ALL_VALUE)
    public ResponseEntity<String> set(@PathVariable("key") String key,
                                      @RequestBody String value,
                                      @RequestHeader(value = SESSION_HEADER, required = false) String sessionId,
                                      HttpServletRequest request){
        // Size check FIRST, before the rate limit: an oversized body is rejected
        // outright, so it should not also cost the client one of their ten writes.
        //
        // The server accepts up to 8192 bytes per command, line and value together, so
        // its real ceiling depends on how long the key is
        int valueBytes = value == null ? 0 : value.getBytes(StandardCharsets.UTF_8).length;
        if(valueBytes > maxValueBytes){
            LOGGER.warn("rejected {}-byte value for key {} (max {})", valueBytes, key, maxValueBytes);
            return ResponseEntity.status(HttpStatus.PAYLOAD_TOO_LARGE)
                    .body("value is " + valueBytes + " bytes; the limit is " + maxValueBytes);
        }
        ResponseEntity<String> reserved = reservedKey(key);
        if(reserved != null) return reserved;
        ResponseEntity<String> limited = rateLimited(request);
        if(limited != null) return limited;
        // 403 rather than 404: the key exists, you simply are not its owner. The UI
        // already knows this and greys the controls; this is the backstop for when it
        // does not, or when somebody curls the endpoint directly.
        if(!keyRegistry.mayWrite(key, sessionId)){
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("key '" + key + "' belongs to another session");
        }
        String response = beedbClient.set(key, value);
        if(response != null && response.equals("STORED")){
            // Only after the cluster confirmed it. Recording first would leave the
            // registry claiming a key that a failed write never created.
            keyRegistry.recordWrite(key, sessionId);
            return ResponseEntity.ok(response);
        }
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
    }

    @DeleteMapping ("/{key}")
    public ResponseEntity<String> delete(@PathVariable("key") String key,
                                         @RequestHeader(value = SESSION_HEADER, required = false) String sessionId,
                                         HttpServletRequest request){
        ResponseEntity<String> reserved = reservedKey(key);
        if(reserved != null) return reserved;
        ResponseEntity<String> limited = rateLimited(request);
        if(limited != null) return limited;
        if(!keyRegistry.mayWrite(key, sessionId)){
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body("key '" + key + "' belongs to another session");
        }
        String response = beedbClient.delete(key);
        if(response != null){
            keyRegistry.recordDelete(key);
            if(response.equals("NOT_FOUND")){
                return ResponseEntity.notFound().build();
            }
            else if(response.equals("DELETED")){
                return ResponseEntity.ok(response);
            }
        }
        // Nothing in the cluster, so drop any stale registry entry too -- a row
        // the store panel shows for a key that is gone is worse than no row.
        keyRegistry.recordDelete(key);
        return ResponseEntity.status(HttpStatus.BAD_GATEWAY).build();
    }
}
