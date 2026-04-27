package com.memcache.cache;

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
                ", value=" + new String(value) +
                ", expiresAt=" + expiresAt +
                ", infiniteExpiry=" + infiniteExpiry +
                ", flags=" + flags +
                '}';
    }
}
