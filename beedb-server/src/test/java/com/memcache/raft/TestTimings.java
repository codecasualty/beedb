package com.memcache.raft;

/**
 * Timing and snapshot parameters shared by every in-process test cluster.
 *
 * WHY THESE NUMBERS
 *
 * Tests want elections that resolve fast, but the ratio between the heartbeat
 * and the election timeout matters far more than either absolute value:
 *
 *     electionTimeoutMin / heartbeatInterval  =  600 / 50  =  6
 *
 * The old values were 150-300ms with a 100ms heartbeat -- a ratio of 1.5, which
 * means a SINGLE delayed heartbeat is enough to start an election. That is fine
 * when a test runs alone and nothing competes for the CPU.
 *
 * These values keep elections sub-second while leaving room for several missed
 * heartbeats. They deliberately do NOT match node*.properties: production sizes
 * its budget around real network and disk latency (1000-2000ms / 300ms)

 */
public final class TestTimings {

    private TestTimings() { }

    /** Randomisation window for the election timeout, milliseconds. */
    public static final int ELECTION_MIN_MS = 300;
    public static final int ELECTION_MAX_MS = 600;

    /** How often a leader sends AppendEntries when it has nothing to say. */
    public static final int HEARTBEAT_MS = 50;

    /** Backoff when a peer is unreachable. Must stay below ELECTION_MIN_MS. */
    public static final int BACKOFF_INITIAL_MS = 200;
    public static final int BACKOFF_MAX_MS     = 1000;

    /** Snapshot aggressively, so compaction and InstallSnapshot are reachable. */
    public static final int SNAPSHOT_LIMIT     = 5;
    public static final int SNAPSHOT_THRESHOLD = 5;

    /** High enough that a test never snapshots -- for exercising the log path. */
    public static final int SNAPSHOT_NEVER = 1000;
}
