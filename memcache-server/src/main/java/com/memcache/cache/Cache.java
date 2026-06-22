package com.memcache.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;

public class Cache {
    
    private ConcurrentHashMap<String, CacheItem> map = new ConcurrentHashMap<>();

    public Cache() {
    }

    public CacheItem get(String key) {
        if (map.containsKey(key)) {
            CacheItem item = map.get(key);
            if (!item.isInfiniteExpiry() && System.currentTimeMillis() >= item.getExpiresAt()) {
                map.remove(key);
                return null;
            }
            return item;
        }
        return null;
    }

    public void put(String key, byte[] value, int flags, int expiry) {
        map.put(key, new CacheItem(key, value, flags, expiry));
    }

    public void remove(String key) {
        map.remove(key);
    }

    public void set(CacheItem item) {
        map.put(item.getKey(), item);
    }

    public boolean containsKey(String key) {
        return this.get(key) != null;
    }

    public Set<String> getKeySet(){
        return map.keySet();
    }
}
