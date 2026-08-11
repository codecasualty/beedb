package com.memcache.raft.wal;
import com.memcache.raft.LogEntry;
import com.memcache.raft.wal.PendingWrite;
import com.memcache.raft.wal.WalRecord;
import com.memcache.raft.wal.WalService;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

public class WalService {
    
    private final        String walFilePath;
    private final        BlockingQueue<PendingWrite> walQueue;
    private              FileChannel walFileChannel;
    private              ExecutorService executorService;
    private final        ObjectMapper objectMapper;
    private final        CRC32 crc32;
    private volatile     long goodSegmentEnd;
    private final        int JSON_LENGTH = 8;
    private final        int CRC_LENGTH = 10;
    private final        Logger LOGGER = LoggerFactory.getLogger(WalService.class.getName());

    public WalService(String walPath) throws IOException{
        this.walFilePath = walPath;
        Path path = Paths.get(walFilePath);
        this.walQueue = new LinkedBlockingQueue<>();
        this.walFileChannel = FileChannel.open(path,StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.executorService = Executors.newSingleThreadExecutor();
        this.objectMapper = new ObjectMapper();
        crc32 = new CRC32();
        goodSegmentEnd = -1;
        this.executorService.submit(() -> start());
        LOGGER.info("wal file path is {}", walFilePath);
    }
    
    public void start(){
        while(true){
            try{
                writeToFile();
            }catch(Exception e){
                e.printStackTrace();
                LOGGER.error("error while writing to file, please check stack trace ", e);
                List<PendingWrite> records = new ArrayList<>();
                walQueue.drainTo(records);
                for(PendingWrite pending : records){
                    pending.future.completeExceptionally(e);
                }
                executorService.shutdown();
                try {
                    executorService.awaitTermination(1000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                    LOGGER.error("error while writing to file, please check stack trace ", e1);
                }
                break;
            }
        }
    }
    
    public CompletableFuture<Void> append(WalRecord walRecord){
        CompletableFuture<Void> future = new CompletableFuture<>();
        walQueue.add(new PendingWrite(walRecord, future));
        return future;
    }
    
    public void writeToFile() throws Exception{
        List<PendingWrite> records = new ArrayList<>();
        PendingWrite pendingWrite = walQueue.take();
        records.add(pendingWrite);
        walQueue.drainTo(records);
        for(PendingWrite pending : records){
            WalRecord walRecord = pending.walRecord;
            // length crc data \n
            // length of only payload
            String json = objectMapper.writeValueAsString(walRecord);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            crc32.reset();
            crc32.update(bytes);
            long checksum = crc32.getValue();
            String emit = String.format("%08d %010d ", bytes.length, checksum) + json + "\n";
            walFileChannel.write(ByteBuffer.wrap(emit.getBytes(StandardCharsets.UTF_8)));
            
        }
        // fsync
        walFileChannel.force(true);
        for(PendingWrite pending : records){
            pending.future.complete(null);
        }

    }
    
    // reutrn a new list of log entries and caller decides either to remove its original copy
    // or add this new copy in its original list
    public List<LogEntry> replayFrom(int index){
        // TODO: read all entries from wal file
        // TODO: from index till last element in wal
        // TODO: read crc and compute crc of the entry
        // TODO: if they do not match then remove all entries from wal from this index till last element
        // TODO: if they match then apply that entry on our raftlog , 
        // TODO: finally return our new raftlog
        List<LogEntry> raftlog = new ArrayList<>();
        try{
            while(walFileChannel.position() < walFileChannel.size()){
                ByteBuffer buffer = ByteBuffer.allocate(JSON_LENGTH + 1 + CRC_LENGTH + 1);
                while(buffer.hasRemaining()){
                    int val = walFileChannel.read(buffer);
                    if(val == -1){
                        break;
                    }
                }
                // for reading flipped
                buffer.flip();
                ByteBuffer lengthBuffer = ByteBuffer.allocate(8);
                while(lengthBuffer.hasRemaining()){
                    lengthBuffer.put(buffer.get());
                }
                buffer.get();

                // figuring out CRC 
                ByteBuffer crcBuffer = ByteBuffer.allocate(10);
                while(crcBuffer.hasRemaining()){
                    crcBuffer.put(buffer.get());
                }
                String crc = new String(crcBuffer.array());
                crc32.reset();
                crc32.update(crcBuffer.array());
                buffer.get();

                // read json body
                ByteBuffer jsonBody = ByteBuffer.allocate(Integer.parseInt(lengthBuffer.array().toString()));
                while(jsonBody.hasRemaining()){
                    walFileChannel.read(jsonBody);
                }
                
                
                // read json string and convert it to WalRecord
                String jsonString = new String(jsonBody.array(), StandardCharsets.UTF_8);
                WalRecord walRecord = objectMapper.readValue(jsonString, WalRecord.class);
                if(crc32.getValue() == (Long.parseLong(crc.trim()))){
                    EntryType entryType = walRecord.entryType;
                    LogEntry logEntry = walRecord.logEntry;
                    int fromIndex = walRecord.fromIndex;
                    if(fromIndex >= index){
                        if (entryType == EntryType.TRUNCATE) {
                            raftlog.subList(fromIndex, raftlog.size()).clear();
                            
                        } else if (logEntry != null) {
                            raftlog.add(logEntry);
                            
                        }
                    }
                }else{
                    goodSegmentEnd = walFileChannel.position() - JSON_LENGTH - 1 - CRC_LENGTH - 1;
                    break;
                }
                // for \n after json body we have to read one more byte
                walFileChannel.position(walFileChannel.position() + 1);
            }
            if(goodSegmentEnd == -1) goodSegmentEnd = walFileChannel.position();
            else{
                walFileChannel.truncate(goodSegmentEnd);
                walFileChannel.position(goodSegmentEnd);
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return raftlog;
        
    }

}
