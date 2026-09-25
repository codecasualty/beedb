package com.memcache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.nio.charset.StandardCharsets;

import org.junit.Test;

import com.memcache.cache.Cache;
import com.memcache.command.Command;
import com.memcache.command.CommandType;
import com.memcache.handler.CommandProcessor;

/**
 * expiry must be decided once, on the leader, and from then on treated as data.
 *
 * before this was fixed each node computed "now + ttl" when it APPLIED a SET, so WAL replay
 * after a restart gave every replayed key a fresh TTL: a restarted node served keys the rest
 * of the cluster had already expired, and currItems differed while lastApplied matched. None
 * of the existing tests touched expiry, which is how every intermediate step of the fix --
 * seconds stored in a millisecond field, a 13-digit value fed to Integer.parseInt -- passed.
 */
public class ExpiryTest {

    private static final long NOW = 1_789_220_000_000L;   


    @Test
    public void relativeSecondsBecomeAnAbsoluteDeadline() {
        assertEquals(NOW + 60_000, Server.absoluteExpiryMillis(60, NOW));
    }

    @Test
    public void zeroStillMeansNeverExpires() {
        assertEquals(0, Server.absoluteExpiryMillis(0, NOW));
    }

    @Test
    public void exactlyThirtyDaysIsStillRelative() {
        long thirtyDays = 30L * 24 * 60 * 60;
        assertEquals(NOW + thirtyDays * 1000, Server.absoluteExpiryMillis(thirtyDays, NOW));
    }

    @Test
    public void aboveThirtyDaysIsAnAbsoluteUnixTime() {
        long unixSeconds = 1_800_000_000L;  
        assertEquals(unixSeconds * 1000, Server.absoluteExpiryMillis(unixSeconds, NOW));
    }

    @Test
    public void negativeIsAlreadyExpired() {
        assertTrue(Server.absoluteExpiryMillis(-1, NOW) < NOW);
    }


    @Test
    public void aMillisecondDeadlineSurvivesSerializeAndDeserialize() {
        Command original = set("k", "hello", 1_789_221_600_419L);
        Command copy = Command.deserialize(original.serialize());
        assertEquals(1_789_221_600_419L, copy.getExpiry());
    }


    @Test
    public void aKeyWithAFutureDeadlineCanBeReadBack() throws Exception {
        Cache cache = new Cache();
        apply(cache, set("k", "hello", System.currentTimeMillis() + 60_000));
        assertNotNull("a key with a TTL vanished the moment it was written", cache.get("k"));
        assertEquals(1, cache.size());
    }

    @Test
    public void aKeyPastItsDeadlineIsNeitherReadNorCounted() throws Exception {
        Cache cache = new Cache();
        apply(cache, set("k", "hello", System.currentTimeMillis() - 1_000));
        assertNull(cache.get("k"));
        assertEquals(0, cache.size());
    }

    @Test
    public void applyingTheSameEntryLaterGivesTheSameDeadline() throws Exception {
        // expiry time shouldn't change
        String logEntry = set("k", "hello", System.currentTimeMillis() + 60_000).serialize();
        Cache early = new Cache();
        apply(early, Command.deserialize(logEntry));
        Thread.sleep(25);
        Cache late = new Cache();
        apply(late, Command.deserialize(logEntry));
        assertEquals(early.get("k").getExpiresAt(), late.get("k").getExpiresAt());
    }


    private static Command set(String key, String value, long expiry) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        Command command = new Command(CommandType.SET, key, 0, expiry, bytes.length);
        command.setValue(bytes);
        return command;
    }

    private static void apply(Cache cache, Command command) throws Exception {
        CommandProcessor.process(command, cache);
    }
}
