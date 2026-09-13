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

    public CacheItem(String key, byte[] value, int flags, long expiry) {
        this.key                = key;
        this.value              = value;
        this.flags              = flags;
        // expiry is already an absolute epoch-millisecond deadline, set once by the leader.
        // Never read the clock here: applying an entry must give the same result on every node
        // and on every replay.
        this.expiresAt          = (expiry == 0) ? -1 : expiry;
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
