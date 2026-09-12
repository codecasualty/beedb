package com.memcache.gateway.demo;

import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.memcache.gateway.client.BeedbClient;
import com.memcache.gateway.client.beedbexception.BeedbTimeoutException;
import com.memcache.gateway.client.beedbexception.NoLeaderException;
import com.memcache.gateway.client.beedbexception.NodeUnreachableException;
import com.memcache.gateway.kv.KeyRegistry;

@Service 
public class DemoWriter {

    // every demo key starts with this, and KvController refuses visitor writes to it.
    // both sides use this one constant, so the guard can never check a different prefix.
    public static final String KEY_PREFIX = "demo:";
    // owner recorded in the KeyRegistry. Anyone can send this as their session header,
    // which is why KvController blocks the prefix instead of relying on ownership.
    private static final String OWNER = "demo-session";
    private static final int RING_SIZE = 20;

    private final AtomicLong ok = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    // a timeout is not a failure: the leader may have committed the write after we
    // stopped waiting, so it is counted apart from the writes the cluster refused.
    private final AtomicLong unconfirmed = new AtomicLong();
    // only this method touches it, and fixedDelay never runs it twice at once.
    private long attempts = 0;
    // written here, read by the status tick on another thread.
    private volatile String lastKey = null;
    private final BeedbClient beedbClient;
    private final KeyRegistry keyRegistry;
    private static final Logger LOGGER = LoggerFactory.getLogger(DemoWriter.class);

    public DemoWriter(BeedbClient beedbClient, KeyRegistry keyRegistry){
        this.beedbClient = beedbClient;
        this.keyRegistry = keyRegistry;
    }

    @Scheduled (fixedDelay = 1000)
    public void write(){
        long n = attempts++;
        String key = KEY_PREFIX + (n % RING_SIZE);
        String value = "write #" + (n + 1);
        lastKey = key;
        // Expected failures log at DEBUG: BeedbClient already logs one WARN line per failed
        // request, and a failover would otherwise add a stack trace every second.
        try{
            String response = beedbClient.set(key, value);
            if("STORED".equals(response)){
                // Only after the cluster confirmed it, same rule as KvController.
                keyRegistry.recordWrite(key, OWNER);
                ok.incrementAndGet();
            }else if(response != null && response.startsWith("SERVER_ERROR") && response.contains("timeout")){
                // The server's own propose timeout: the entry may still commit.
                unconfirmed.incrementAndGet();
                LOGGER.debug("demo write {} unconfirmed: {}", key, response);
            }else{
                failed.incrementAndGet();
                LOGGER.debug("demo write {} failed: {}", key, response);
            }
        }catch(NoLeaderException | NodeUnreachableException e){
            failed.incrementAndGet();
            LOGGER.debug("demo write {} failed: {}", key, e.getMessage());
        }catch(BeedbTimeoutException e){
            unconfirmed.incrementAndGet();
            LOGGER.debug("demo write {} unconfirmed: {}", key, e.getMessage());
        }catch(Exception e){
            // Not a failover but a bug. Spring would keep the task running either way;
            // catching it keeps the attempt counted and the trace in the log.
            failed.incrementAndGet();
            LOGGER.warn("demo write {} failed unexpectedly", key, e);
        }
    }

    public DemoRecord view(){
        return new DemoRecord(ok.get(), failed.get(), unconfirmed.get(), lastKey);
    }
}
