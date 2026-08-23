package com.memcache.raft.wal;
import com.memcache.raft.LogEntry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
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
        createFileWithPermissions(walFilePath); 
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
        while(!Thread.currentThread().isInterrupted()){
            try{
                writeToFile();
            }catch(Exception e){
                // LOGGER.error("something is wrong {}",e);
                LOGGER.error("file in which write failed is {}", walFilePath);
                LOGGER.error("error while writing to file, please check stack trace ", e);
                shutdown();
                break;
            }
        }
    }
    
    public CompletableFuture<Void> append(WalRecord walRecord){
        CompletableFuture<Void> future = new CompletableFuture<>();
        walQueue.add(new PendingWrite(walRecord, future));
        LOGGER.info("appending an entry in wal queue , entry is {}", walRecord);
        LOGGER.info("appending an entry in wal queue, wal queue size is {}", walQueue.size());
        return future;
    }
    
    public void writeToFile() throws Exception{
        CRC32 crc32 = new CRC32();
        List<PendingWrite> records = new ArrayList<>();
        PendingWrite pendingWrite = null;
        // take is blocking and interruptible call in nature, so when we try to close the resource and this thread is 
        // waiting for take to complete , it will be interrupted and we will get an exception
        // therefore its good to use try and cathc block
        try{
            pendingWrite = walQueue.take();
        }
        catch(InterruptedException e){
            Thread.currentThread().interrupt();
            // LOGGER.error("error while taking from queue , please check stack trace ", e);
            return ;
        } 
        records.add(pendingWrite);
        walQueue.drainTo(records);
        LOGGER.info("we have taken the records from queue");
        for(PendingWrite pending : records){
            WalRecord walRecordDummy = pending.walRecord;
            if(walRecordDummy.getEntryType() == EntryType.COMPACT){
                compact(walRecordDummy);
                continue;
            }
            WalRecord walRecord = null;
            LogEntry logEntryDummy = walRecordDummy.logEntry;
            if(logEntryDummy == null) walRecord = walRecordDummy;
            else{
                LogEntry logEntry = new LogEntry(logEntryDummy.getIndex(), logEntryDummy.getCommand(), logEntryDummy.getTerm(), logEntryDummy.isNoOp(), "");
                walRecord = new WalRecord(walRecordDummy.entryType, logEntry, walRecordDummy.fromIndex);
            }
            // length crc data \n
            // length of only payload
            String json = objectMapper.writeValueAsString(walRecord);
            byte[] bytes = json.getBytes(StandardCharsets.UTF_8);
            crc32.reset();
            crc32.update(bytes);
            long checksum = crc32.getValue();
            String emit = String.format("%08d %010d ", bytes.length, checksum) + json + "\n";
            LOGGER.info("going to write in files {} ", emit);
            walFileChannel.write(ByteBuffer.wrap(emit.getBytes(StandardCharsets.UTF_8)));
            LOGGER.info("wal record is {} written to file", walRecord);
            
        }
        // fsync
        walFileChannel.force(true);
        LOGGER.info("we have written all the records in queue");
        for(PendingWrite pending : records){
            pending.future.complete(null);
        }
        LOGGER.info("we have completed all the futures in queue");

    }

    public void compact(WalRecord walRecord){
        int compactIndex = walRecord.getFromIndex();
        LOGGER.info("compacting till index {} ", compactIndex);
        Path path = Path.of(walFilePath);
        String tempFilePath = "/tmp"+path.getParent().toString()+"/"+"wal.temp";
        LOGGER.info("temp file path is {}", tempFilePath);
        createFileWithPermissions(tempFilePath);
        // now we will copy all entries from wal only from compactIndex till last element in wal
        // but during this time , we wont be carrying any inserts in our original wal file
        // so we will be writing all the entries in temp file
        try(
            
            FileChannel tempFileChannel = FileChannel.open(Path.of(tempFilePath), StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        ){    
            LOGGER.info("***************printing wal file channel before compaction ");
            print(walFileChannel);
            walFileChannel.position(0);
            while(walFileChannel.position() < walFileChannel.size()){
                // we have entry in format 20 + json body (spanned over length of json body) + \n
                // so we will read 20 bytes and then read length of json body and then read \n
                ByteBuffer buffer = ByteBuffer.allocate(8);
                while(buffer.hasRemaining()){
                    int val = walFileChannel.read(buffer);
                    if(val == -1){
                        break;
                    }
                }
                // we dont need to verify if our buffer is valid or not , because we are reading from wal file
                buffer.flip();
                int length = Integer.parseInt(new String(buffer.array(), StandardCharsets.UTF_8));
                ByteBuffer remainingBytes = ByteBuffer.allocate(12);
                while(remainingBytes.hasRemaining()){
                    int val = walFileChannel.read(remainingBytes);
                    if(val == -1)break;
                }
                remainingBytes.flip();
                // adding one extra byte to read \n
                ByteBuffer jsonBody = ByteBuffer.allocate(length+1);
                while(jsonBody.hasRemaining()){
                    int read = walFileChannel.read(jsonBody);
                    if(read == -1){
                        break;
                    }
                }
                jsonBody.flip();
                String jsonString = new String(jsonBody.array(), StandardCharsets.UTF_8);
                // LOGGER.info("json string is {}", jsonString);
                // parsing into walrecord
                WalRecord currentWalRecord = objectMapper.readValue(jsonString, WalRecord.class);
                if(
                    (currentWalRecord.getEntryType() == EntryType.ENTRY && currentWalRecord.getLogEntry().getIndex() > compactIndex) ||
                    (currentWalRecord.getEntryType() == EntryType.TRUNCATE && currentWalRecord.getFromIndex() > compactIndex)
                ){
                    // LOGGER.info("printing buffers");
                    // LOGGER.info("buffer start pointer is {} and limit is {} ", buffer.position(), buffer.limit());
                    // printBuffer(buffer.duplicate());
                    tempFileChannel.write(buffer);
                    // LOGGER.info("remaining bytes start pointer is {} and limit is {} ", remainingBytes.position(), remainingBytes.limit());
                    // printBuffer(remainingBytes.duplicate());
                    tempFileChannel.write(remainingBytes);
                    // LOGGER.info("json body start pointer is {} and limit is {} ", jsonBody.position(), jsonBody.limit());
                    // printBuffer(jsonBody.duplicate());
                    tempFileChannel.write(jsonBody);
                    // LOGGER.info("this json string is inserted in tempfile channel {} ", jsonString);
                    // LOGGER.info("printing buffers done");
                    // print(tempFileChannel);
                    // LOGGER.info("entry is written to temp file channel");
                }
                // LOGGER.info("printing tempfile channle after each entry is written ");
                // print(tempFileChannel);
                // walFileChannel.position(walFileChannel.position() + 1);
            }
            LOGGER.info("printing temp file channel after all entries are written");
            print(tempFileChannel);
            tempFileChannel.force(true);
            tempFileChannel.close();
            // Thread.sleep(100);
            // FileChannel duplicateFileChannel = FileChannel.open(Path.of(tempFilePath), StandardOpenOption.READ, StandardOpenOption.WRITE);
            // LOGGER.info("printing duplicate file channel after temp file is forced");
            // print(duplicateFileChannel);
            Path sourcePath = Path.of(tempFilePath);
            Path targetPath = Path.of(walFilePath);
            // LOGGER.info("moving file from {} to {}", sourcePath, targetPath);
            Files.move(sourcePath, targetPath, StandardCopyOption.ATOMIC_MOVE);
            // LOGGER.info("file moved ");
            walFileChannel.close();
            // once we are opening an file channel , by default it will start reading from the beginning of the file
            walFileChannel = FileChannel.open(Path.of(walFilePath),StandardOpenOption.READ, StandardOpenOption.WRITE);
            // therefore we will start writing from the end of the file
            walFileChannel.position(walFileChannel.size());
        }catch(Exception e){
            // LOGGER.error("something is wrong {}",e);
            throw new RuntimeException(e);
        }
        
        // try{
        //     LOGGER.info("checking what is stoerd in filechannel after compaction ");
        //     print(FileChannel.open(Path.of(walFilePath), StandardOpenOption.READ));

        // }catch(Exception e){
        //     LOGGER.error("something is wrong {}",e);
        // }
        // we have to reopen our actual file channel 
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
        long recordStart = 0;

        try{
            LOGGER.info("checking what is stoerd in filechannel before replay is being done ");
            print(walFileChannel);
            LOGGER.info("from index is {} ", index);
            LOGGER.info("position/end pointer is {} ", walFileChannel.position());
            LOGGER.info("size of file is {} ", walFileChannel.size());
            LOGGER.info("good segment end is {} ", goodSegmentEnd);
            walFileChannel.position(0);
            while(walFileChannel.position() < walFileChannel.size()){
                recordStart = walFileChannel.position();
                // LOGGER.info("start of record is {} ", recordStart);
                // LOGGER.info(" in while loop position is {} ", walFileChannel.position());
                ByteBuffer buffer = ByteBuffer.allocate(JSON_LENGTH + 1 + CRC_LENGTH + 1);
                while(buffer.hasRemaining()){
                    int val = walFileChannel.read(buffer);
                    if(val == -1){
                        break;
                    }
                }
                if(buffer.hasRemaining()){
                    // LOGGER.info("buffer has remaining bytes {} and therefore we are breaking the loop", buffer.hasRemaining());
                    truncate(recordStart);
                    break;
                }
                // for reading flipped
                buffer.flip();
                // printBuffer(buffer.duplicate());
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
                String lengthString = new String(lengthBuffer.array() , StandardCharsets.UTF_8);
                lengthString = lengthString.trim();
                long remaining = walFileChannel.size() - walFileChannel.position();
                // LOGGER.info("length string {} is not a valid integer or remaining bytes {} < length of string {}", lengthString, remaining, lengthString.length());
                if(!lengthString.matches("\\d+")  || remaining < lengthString.length()){
                    // LOGGER.info("length string {} is not a valid integer or remaining bytes {} < length of string {}", lengthString, remaining, lengthString.length());
                    truncate(recordStart);
                    break;
                }
                int lengthJson = Integer.parseInt(lengthString);
                ByteBuffer jsonBody = ByteBuffer.allocate(lengthJson);
                // LOGGER.info("json body end pointer is {} ", jsonBody.position());
                while(jsonBody.hasRemaining()){
                    int read = walFileChannel.read(jsonBody);
                    if(read == -1){
                        break;
                    }
                }
                // LOGGER.info("json body end pointer is {} ", jsonBody.position());

                if(jsonBody.hasRemaining()){
                    // LOGGER.info("json body has remaining bytes {} and therefore we are breaking the loop , json end pointer is {} ", jsonBody.hasRemaining(), jsonBody.position());
                    truncate(recordStart);
                    break;
                }
                // read json string and convert it to WalRecord
                String jsonString = new String(jsonBody.array(), StandardCharsets.UTF_8);
                // LOGGER.info("json string while reading from file is {}", jsonString);
                // parse and verify json string, this is necessary because while writing someone might have corrupted the json string
                // and we only support utf-8 encoding
                byte[] utf8Bytes = jsonString.getBytes(StandardCharsets.UTF_8);
                String reEncodedJsonString = new String(utf8Bytes, StandardCharsets.UTF_8);
                if(!jsonString.equals(reEncodedJsonString)){
                    // LOGGER.error("corrupted json string {} ", jsonString);
                    truncate(recordStart);
                    break;
                }
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
                    WalRecord walRecord = objectMapper.readValue(jsonString, WalRecord.class);
                    EntryType entryType = walRecord.entryType;
                    LogEntry logEntry = walRecord.logEntry;
                    int fromIndex = walRecord.fromIndex;
                    // LOGGER.info("log entry index index is {} and index from where we are reading is {} ", (logEntry != null ? logEntry.getIndex() : -1), index);
                    if(entryType == EntryType.TRUNCATE && fromIndex > index) {                        
                        raftlog.removeIf(e -> e.getIndex() >= fromIndex);
                        
                    } else if (logEntry != null && logEntry.getIndex() > index) {
                        raftlog.add(logEntry);                            
                    }
                }else{
                    goodSegmentEnd = recordStart ;
                    truncate(goodSegmentEnd);
                    break;
                }
                // LOGGER.info("printing raftlog in replay from method at each step ");
                // print(raftlog);
                // LOGGER.info("-----------------********-----------------------\n");
                // for \n after json body we have to read one more byte
                // the reason for this is that we are reading the json body and we have to read the \n after json body
                // and if we don't read \n then we will be reading the next record
                // also we are doing this after completoin of complete logic because at this point of time, we are sure that 
                // our wal is valid
                // LOGGER.info("end of record is {} ", walFileChannel.position());
                walFileChannel.position(walFileChannel.position() + 1);
            }
            // LOGGER.info("position/end pointer is {} ", walFileChannel.position());
            // LOGGER.info("size of file is {} ", walFileChannel.size());
        }catch(Exception e){
            // LOGGER.error("something is wrong {}",e);
            throw new RuntimeException(e);
        }
        goodSegmentEnd = -1;
        LOGGER.info("----------------------------  wal size is {} ", raftlog.size());
        for(LogEntry logEntry : raftlog){
            LOGGER.info("---------------------------- restrored entry  from wal is {}", logEntry);
        }
        return raftlog;
        
    }

    public void truncate(long index){
        try{
            walFileChannel.truncate(index);
            walFileChannel.position(index);
        }catch(Exception e){
            // LOGGER.error("something is wrong {}",e);
            throw new RuntimeException(e);
        }
    }

    public boolean createFileWithPermissions(String filePath){
        Path path = Path.of(filePath);
        Path dirPath = path.getParent();
        try{
            
            Files.createDirectories(dirPath);
            LOGGER.info("directory created at {}", dirPath);
        
            if(!Files.exists(path)){
                Files.createFile(path);
                LOGGER.info("file created at {}", filePath);
            }
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxr-xr-x");
            Files.setPosixFilePermissions(Path.of(filePath), permissions);
        }catch(IOException e){
            // its importatnt to note that we wont get IOexception while creating parent directories or giving them permissions
            // but lets say the dirPath has file in it or we dont have sufficient permissinos or fiel system is read only 
            LOGGER.error("error while directory creation , check stack trace ",e);
            return false;
        }
        return true;
    }
    
    public void shutdown(){
        if(walQueue != null){
            for(PendingWrite pending : walQueue){
                pending.future.completeExceptionally(new RuntimeException("Wal service is shutting down"));
            }
        } 
        if(executorService != null) executorService.shutdownNow();
        if(walFileChannel != null){
            try{
                walFileChannel.close();
            }catch(Exception e){
                LOGGER.error("something is wrong {}",e);
            }
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

    public void print(FileChannel fileChannel) throws IOException{
        long endPointer = fileChannel.position();
        fileChannel.position(0);
        LOGGER.info("end pointer is {} file size is {} ", endPointer, fileChannel.size());
        LOGGER.info("printing file channel ----------------------------------\n");
        ByteBuffer buffer = ByteBuffer.allocate((int)fileChannel.size());
        while(buffer.hasRemaining()){
            int val = fileChannel.read(buffer);
            if(val == -1){
                break;
            }
        }
        buffer.flip();
        printBuffer(buffer);
        fileChannel.position(endPointer);
        LOGGER.info("final position of file channel is {} ", fileChannel.position());
        LOGGER.info("----------------------------------------\n");


    }

    public void printFileChannel() {
        try{
            print(FileChannel.open(Path.of(walFilePath), StandardOpenOption.READ));

        }catch(Exception e){
            LOGGER.error("something is wrong {}",e);
        }
    }

    public long getPosition() throws IOException{
        return walFileChannel.position();
    }
}
