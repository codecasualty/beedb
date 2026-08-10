package com.memcache.raft.wal;
// our entries which will be added in wal will be normal entries with command like
// put , del, get, etc
// or truncate marker to delete entries from particular location till last location in raftlog
public enum EntryType {
    ENTRY, 
    TRUNCATE
}
