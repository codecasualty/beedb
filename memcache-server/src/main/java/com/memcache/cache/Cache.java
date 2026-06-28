package com.memcache.cache;

import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.stream.Collectors;


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

    public Map<String, CacheItem> getState(){
        long now = System.currentTimeMillis();
        return map.entrySet().stream()
                .filter(entry -> entry.getValue().isInfiniteExpiry() || entry.getValue().getExpiresAt() > now)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public void restoreState(Map<String , CacheItem> state){
        map.clear();
        map.putAll(state);
    }

}
