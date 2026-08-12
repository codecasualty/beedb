package com.memcache.raft.wal;
import com.memcache.raft.LogEntry;
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
        CRC32 crc32 = new CRC32();
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
            LOGGER.info("wal record is {} written to file", walRecord);
            
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
        CRC32 crc32 = new CRC32();

        try{
            walFileChannel.position(0);
            LOGGER.info("from index is {} ", index);
            LOGGER.info("position is {} ", walFileChannel.position());
            LOGGER.info("size of file is {} ", walFileChannel.size());
            LOGGER.info("good segment end is {} ", goodSegmentEnd);
            while(walFileChannel.position() < walFileChannel.size()){
                long recordStart = walFileChannel.position();
                // LOGGER.info(" in while loop position is {} ", walFileChannel.position());
                ByteBuffer buffer = ByteBuffer.allocate(JSON_LENGTH + 1 + CRC_LENGTH + 1);
                while(buffer.hasRemaining()){
                    int val = walFileChannel.read(buffer);
                    if(val == -1){
                        break;
                    }
                }
                if(buffer.hasRemaining()){
                    truncate(recordStart);
                    break;
                }
                // for reading flipped
                buffer.flip();
                printBuffer(buffer.duplicate());
                ByteBuffer lengthBuffer = ByteBuffer.allocate(8);
                while(lengthBuffer.hasRemaining()){
                    lengthBuffer.put(buffer.get());
                }
                lengthBuffer.flip();
                // printBuffer(lengthBuffer.duplicate());
                buffer.get();

                // figuring out CRC 
                ByteBuffer crcBuffer = ByteBuffer.allocate(10);
                while(crcBuffer.hasRemaining()){
                    crcBuffer.put(buffer.get());
                }
                String crc = new String(crcBuffer.array(), StandardCharsets.UTF_8);
                // LOGGER.info("crc stored in wal entry is  {} ", crc);
                buffer.get();
                
                // read json body
                // LOGGER.info("length of json body is {} ", Integer.parseInt(new String(lengthBuffer.array() , StandardCharsets.UTF_8)));
                int lengthJson = Integer.parseInt(new String(lengthBuffer.array() , StandardCharsets.UTF_8));
                ByteBuffer jsonBody = ByteBuffer.allocate(lengthJson);
                while(jsonBody.hasRemaining()){
                    int read = walFileChannel.read(jsonBody);
                    if(read == -1){
                        break;
                    }
                }

                if(jsonBody.hasRemaining()){
                    truncate(recordStart);
                    break;
                }
                // read json string and convert it to WalRecord
                String jsonString = new String(jsonBody.array(), StandardCharsets.UTF_8);
                // parse and verify json string, this is necessary because while writing someone might have corrupted the json string
                // and we only support utf-8 encoding
                byte[] utf8Bytes = jsonString.getBytes(StandardCharsets.UTF_8);
                String reEncodedJsonString = new String(utf8Bytes, StandardCharsets.UTF_8);
                if(!jsonString.equals(reEncodedJsonString)){
                    truncate(recordStart);
                    break;
                }
                WalRecord walRecord = objectMapper.readValue(jsonString, WalRecord.class);
                // computing crc value for wal record
                byte[] bytes = jsonString.getBytes(StandardCharsets.UTF_8);
                crc32.reset();
                crc32.update(bytes);
                long checksum = crc32.getValue();

                // LOGGER.info("wal record is {} read from file", walRecord);
                // LOGGER.info("new computed crc is {} ", checksum);
                // LOGGER.info("stored crc is {} ", (Long.parseLong(crc.trim())));
                // LOGGER.info("entry type is {} ", walRecord.entryType);
                if(checksum == (Long.parseLong(crc.trim()))){
                    EntryType entryType = walRecord.entryType;
                    LogEntry logEntry = walRecord.logEntry;
                    int fromIndex = walRecord.fromIndex;
                    // LOGGER.info("log entry index index is {} and index from where we are reading is {} ", (logEntry != null ? logEntry.getIndex() : -1), index);
                    if(entryType == EntryType.TRUNCATE && fromIndex > index) {                        
                        raftlog.removeIf(e -> e.getIndex() >= fromIndex);
                        
                    } else if (logEntry != null && logEntry.getIndex() > index) {
                        raftlog.add(logEntry);                            
                    }
                    
                    // print(raftlog);
                }else{
                    goodSegmentEnd = recordStart ;
                    truncate(goodSegmentEnd);
                    break;
                }
                // for \n after json body we have to read one more byte
                walFileChannel.position(walFileChannel.position() + 1);
                // LOGGER.info("-----------------********-----------------------\n");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        goodSegmentEnd = -1;
        return raftlog;
        
    }

    public void truncate(long index){
        try{
            walFileChannel.truncate(index);
            walFileChannel.position(index);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    public void print(List<LogEntry> raftlog){
        LOGGER.info("--------------------------------------printing log entries ----------------------------------\n");
        for(LogEntry logEntry : raftlog){
            LOGGER.info("log entry is {}", logEntry);
        }
        LOGGER.info("size of raftlog is {}", raftlog.size());
        LOGGER.info("----------------------------------------\n");
    }

    public void printBuffer(ByteBuffer buffer){
        LOGGER.info("printing buffer ----------------------------------\n");
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        String str = new String(bytes, StandardCharsets.UTF_8);
        LOGGER.info("buffer is {} ", str);
        LOGGER.info("----------------------------------------\n");
    }
}
