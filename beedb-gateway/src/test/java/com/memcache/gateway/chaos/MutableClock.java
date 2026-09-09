package com.memcache.gateway.chaos;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

/**
 * a Clock whose "now" we set by hand.
 *
 * this is the whole reason ChaosService takes a Clock instead of calling Instant.now().
 * nothing else here is thread-safe -- advance the clock from one thread only.
 */
public final class MutableClock extends Clock {

    private volatile Instant now;

    public MutableClock(Instant start) {
        this.now = start;
    }

    /** A fixed, readable starting point so failure messages show recognisable times. */
    public static MutableClock atEpochStart() {
        return new MutableClock(Instant.parse("2026-01-01T00:00:00Z"));
    }

    /** Jump forward. Returns this, so it chains inside a test line. */
    public MutableClock advanceSeconds(long seconds) {
        now = now.plusSeconds(seconds);
        return this;
    }

    public MutableClock set(Instant instant) {
        now = instant;
        return this;
    }

    @Override public Instant instant()             { return now; }
    @Override public ZoneId  getZone()             { return ZoneOffset.UTC; }
    @Override public Clock   withZone(ZoneId zone) { return this; }
}
