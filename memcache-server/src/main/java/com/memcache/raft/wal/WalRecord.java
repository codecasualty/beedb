
package com.memcache.raft.wal;

import com.memcache.raft.LogEntry;

public class WalRecord {

    EntryType entryType;
    LogEntry logEntry;
    int fromIndex;
    
}
