package com.memcache.raft.wal;
// our entries which will be added in wal will be normal entries with command like
// put , del, get, etc
// or truncate marker to delete entries from particular location till last location in raftlog
// TRUNCATE will truncate entries from particular index to last index in raftlog
// COMPACT will compact the wal file by removing all the entries from start of log till the given index (last included index / last applied index)
public enum EntryType {
    ENTRY, 
    TRUNCATE,
    COMPACT
}
