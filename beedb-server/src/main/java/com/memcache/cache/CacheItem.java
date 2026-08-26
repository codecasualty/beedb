package com.memcache.cache;

import java.nio.charset.StandardCharsets;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CacheItem {
    private String      key;
    private byte[]      value;
    private long        expiresAt;
    private boolean     infiniteExpiry;
    private int         flags;

    public CacheItem(String key, byte[] value, int flags, int expiry) {
        this.key                = key;
        this.value              = value;
        this.flags              = flags;
        this.expiresAt          = (expiry == 0) ? -1 : System.currentTimeMillis() + (expiry * 1000L);
        this.infiniteExpiry     = (expiry == 0);
    }

    @JsonCreator
    public CacheItem(
        @JsonProperty("key") String key, 
        @JsonProperty("value") byte[] value, 
        @JsonProperty("flags") int flags,
        @JsonProperty("expiresAt") long expiresAt,
        @JsonProperty("infiniteExpiry") boolean infiniteExpiry
    ){
        this.key = key;
        this.value = value;
        this.flags = flags;
        this.expiresAt = expiresAt;
        this.infiniteExpiry = infiniteExpiry;
    }

    public String getKey() {
        return key;
    }

    public byte[] getValue() {
        return value;
    }

    public long getExpiresAt() {
        return expiresAt;
    }

    public boolean isInfiniteExpiry() {
        return infiniteExpiry;
    }

    public int getFlags() {
        return flags;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public String toString() {
        return "CacheItem{" +
                "key='" + key + '\'' +
                ", value=" + new String(value, StandardCharsets.UTF_8) +
                ", expiresAt=" + expiresAt +
                ", infiniteExpiry=" + infiniteExpiry +
                ", flags=" + flags +
                '}';
    }
}
