package com.memcache.command;

public class Command {
    
    private CommandType type;
    private String      key;
    private int         flags;
    private int         expiry;
    private int         byteLength;
    private byte[]      value;

    public Command(CommandType type, String key, int flags, int expiry, int byteLength) {
        this.type      = type;
        this.key       = key;
        this.flags     = flags;
        this.expiry    = expiry;
        this.byteLength = byteLength;
    }

    public CommandType getType() {
        return type;
    }

    public String getKey() {
        return key;
    }

    public int getFlags() {
        return flags;
    }

    public int getExpiry() {
        return expiry;
    }

    public int getByteLength() {
        return byteLength;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public void setExpiry(int expiry) {
        this.expiry = expiry;
    }

    public String toString() {
        return "Command{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", flags=" + flags +
                ", expiry=" + expiry +
                ", byteLength=" + byteLength +
                ", value=" + new String(value) +
                '}';
    }
}
