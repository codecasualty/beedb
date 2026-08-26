package com.memcache.raft.wal;

import java.util.concurrent.CompletableFuture;

public class PendingWrite {
    WalRecord walRecord;
    CompletableFuture<Void> future;

    public PendingWrite(WalRecord walRecord, CompletableFuture<Void> future){
        this.walRecord = walRecord;
        this.future = future;
    }
}
