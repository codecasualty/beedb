package com.memcache.command;

import java.util.Base64;

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

    public String serialize() {
        String encodedValue = value != null ? Base64.getEncoder().encodeToString(value) : "";
        return type + "\t" + key + "\t" + flags + "\t" + expiry + "\t" + byteLength + "\t" + encodedValue;
    }

    public static Command deserialize(String s) {
        String[] parts = s.split("\t", 6);
        CommandType type = CommandType.valueOf(parts[0]);
        String key = parts[1];
        int flags = Integer.parseInt(parts[2]);
        int expiry = Integer.parseInt(parts[3]);
        int byteLength = Integer.parseInt(parts[4]);
        Command cmd = new Command(type, key, flags, expiry, byteLength);
        if (parts.length == 6 && !parts[5].isEmpty()) {
            cmd.setValue(Base64.getDecoder().decode(parts[5]));
        }
        return cmd;
    }

    public String toString() {
        String strValue = value != null ? new String(value) : "null";
        return "Command{" +
                "type=" + type +
                ", key='" + key + '\'' +
                ", flags=" + flags +
                ", expiry=" + expiry +
                ", byteLength=" + byteLength +
                ", value=" + strValue +
                '}';
    }
}
