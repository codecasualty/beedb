package com.memcache.raft.wal;
import com.memcache.raft.LogEntry;
import com.memcache.raft.wal.PendingWrite;
import com.memcache.raft.wal.WalRecord;
import com.memcache.raft.wal.WalService;

import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.CRC32;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WalService {
    
    private String walFilePath;
    private BlockingQueue<PendingWrite> walQueue;
    private FileChannel walFileChannel;
    private ExecutorService executorService;
    private ObjectMapper objectMapper;
    private CRC32 crc32;
    
    public WalService(String walFilePath){
        this.walFilePath = walFilePath;
        this.walQueue = new LinkedBlockingQueue<>();
        this.walFileChannel = null;
        this.executorService = Executors.newSingleThreadExecutor();
        this.objectMapper = new ObjectMapper();
        crc32 = new CRC32();
    }
    
    public void start(){
        while(true){
            writeToFile();
        }
    }
    
    public CompletableFuture<Void> append(WalRecord walRecord){
        CompletableFuture<Void> future = new CompletableFuture<>();
        walQueue.add(new PendingWrite(walRecord, future));
        return future;
    }
    
    public void writeToFile(){
        List<PendingWrite> records = new ArrayList<>();
        try{
            PendingWrite pendingWrite = walQueue.take();
            records.add(pendingWrite);
            walQueue.drainTo(records);
            for(PendingWrite pending : records){
                WalRecord walRecord = pending.walRecord;
                // length crc data \n
                String json = objectMapper.writeValueAsString(walRecord);
                byte[] bytes = json.getBytes();
                crc32.update(bytes);
                long checksum = crc32.getValue();
                String emit = String.format("%08d %010d ", bytes.length, checksum) + json + "\n";
                walFileChannel.write(emit.getBytes());
                /*
                json  = mapper.writeValueAsString(walRecord)
                bytes = json.getBytes(UTF_8)
                crc   = CRC32 over bytes
                emit  = String.format("%08d %010d ", bytes.length, crc) + json + "\n"
                channel.write(emit.getBytes(UTF_8))
                 */

                
            }
        }catch(Exception e){
            e.printStackTrace();
        }

    }
    
    public List<LogEntry> replayFrom(int index, List<LogEntry> raftLog){
        // TODO: read all entries from wal file
        // TODO: from index till last element in wal
        // TODO: read crc and compute crc of the entry
        // TODO: if they do not match then remove all entries from wal from this index till last element
        // TODO: if they match then apply that entry on our raftlog , 
        // TODO: finally return our new raftlog
        return null;
    }

}
