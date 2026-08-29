package com.memcache.raft.wal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.command.CommandType;
import com.memcache.raft.LogEntry;

public class WalServiceTest {
    
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private WalService walService;
    private Logger LOGGER = LoggerFactory.getLogger(WalServiceTest.class.getName());
    private String walPath;
    private String tmpDir;
    @Before
    public void setUp() throws Exception {
        walPath = folder.newFile("wal.log").getAbsolutePath();
        tmpDir  = folder.toString();
        walService = new WalService(walPath, tmpDir);
    }

    /*
    1. insert some entry in wal to check append is working 
    2. read from particular index 
    3. truncate from particular index
    4. corrupt some entry in wal
     */

    public List<CompletableFuture<Void>> insert(){
        CompletableFuture<Void> future1 = walService.append(new WalRecord(EntryType.ENTRY, new LogEntry(1, CommandType.SET.toString(), 0, false, UUID.randomUUID().toString()), 0));
        CompletableFuture<Void> future2 = walService.append(new WalRecord(EntryType.ENTRY, new LogEntry(2, CommandType.SET.toString(), 0, false, UUID.randomUUID().toString()), 0));
        CompletableFuture<Void> future3 = walService.append(new WalRecord(EntryType.ENTRY, new LogEntry(3, CommandType.SET.toString(), 0, false, UUID.randomUUID().toString()), 0));
        CompletableFuture<Void> future4 = walService.append(new WalRecord(EntryType.ENTRY, new LogEntry(4, CommandType.SET.toString(), 0, false, UUID.randomUUID().toString()), 0));
        CompletableFuture<Void> future5 = walService.append(new WalRecord(EntryType.TRUNCATE, null, 2));
        future1.join();
        future2.join();
        future3.join();
        future4.join();
        future5.join();
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        futures.add(future1);
        futures.add(future2);
        futures.add(future3);
        futures.add(future4);
        futures.add(future5);
        return futures;
    }
    
    @Test
    public void testAppend(){
        List<CompletableFuture<Void>> futures = insert();
        // we have to check if all entries are successfully appended in wal
        for(CompletableFuture<Void> future : futures){
            assert future.isDone();
            assert future.isCompletedExceptionally() == false;
        }
    }

    @Test
    public void testRead(){
        List<CompletableFuture<Void>> futures = insert();
        // we have read entries from particular index
        List<LogEntry> raftlog = walService.replayFrom(3);
        assertTrue(raftlog.size() == 1);
        List<LogEntry> raftlog1 = walService.replayFrom(1);
        LOGGER.debug("raftlog1 is {}", raftlog1);
        LOGGER.debug("size of raftlog1 is {}", raftlog1.size());
        for(LogEntry logEntry : raftlog1){
            LOGGER.debug("log entry is {}", logEntry);
        }
        assertTrue(raftlog1.size() == 0);


    }

    // public void testTruncate(){
        
    // }
    @Test
    public void testCorruptedJSONBody() throws Exception{
        /*
        some methods to take care of 
        raf.length() returns the length of file its pointing to , if during its reading , internal file is truncated then 
        it will reflect that in length
        raf.getFilePointer() returns the current position of file pointer
         */
        insert();
        assertEquals(new File(walPath).length(), walService.getPosition());
        LOGGER.debug("position of wal file is {} ", walService.getPosition());
        RandomAccessFile raf = writeToFile();
        LOGGER.debug("length of raf pointer to file is {} ", raf.length());
        raf.write('X');
        List<LogEntry> raftlog = walService.replayFrom(0);
        assertTrue(raftlog.size() == 1);
        assertTrue(raftlog.get(0).getIndex() == 1);
        LOGGER.debug("final position of wal file is {} ", walService.getPosition());
        LOGGER.debug("position of raf {} ", raf.getFilePointer());
        LOGGER.debug("raf length {} ", raf.length());
        // these length will be equal because eXtryType is not valid key in json and therefore wal will be truncated
        // and we are 1 extra pointer from last character + 20 header bytes + 3 bytes of corrupted json body
        assertEquals(raf.getFilePointer() - 20 - 3 - 1, walService.getPosition());
    }

    @Test
    public void testCorruptedNonUTF8Body() throws Exception{
        insert();
        assertEquals(new File(walPath).length(), walService.getPosition());
        RandomAccessFile raf = writeToFile();
        raf.write(0x00);
        List<LogEntry> raftlog = walService.replayFrom(0);
        assertTrue(raftlog.size() == 1);
        assertTrue(raftlog.get(0).getIndex() == 1);
        LOGGER.debug("final position of wal file is {} ", walService.getPosition());
        LOGGER.debug("raf pointer is at {} ", raf.getFilePointer());
        LOGGER.debug("raf length {} ", raf.length());
        // these length will be equal because after corruption we will truncate complete rest of string from wal file
        assertEquals(raf.getFilePointer() - 20 - 3 - 1, walService.getPosition());
    }

    @Test
    public void testInsertAfterCorruptedJSONBody() throws Exception{
        insert();
        RandomAccessFile raf = writeToFile();
        raf.write('X');
        List<LogEntry> raftlog = walService.replayFrom(0);
        assertTrue(raftlog.size() == 1);
        assertTrue(raftlog.get(0).getIndex() == 1);
        assertEquals(raf.getFilePointer() - 20 - 3 - 1, walService.getPosition());
        // insert some new entries
        // after truncation we should have only one entry and new entries shoudl be appended after first entry

        insert();
        raftlog = walService.replayFrom(0);
        // here we are checking for 2 because we are inserting 2 same entries i.e. LogEntry(1) LogEntry(1) LogEntry(2) LogEntry(3)....
        assertTrue(raftlog.size() == 2);
        assertTrue(raftlog.get(0).getIndex() == 1);
        assertTrue(raftlog.get(1).getIndex() == 1);

    }

    public RandomAccessFile writeToFile() throws Exception{
        RandomAccessFile raf = new RandomAccessFile(walPath, "rw");
        byte[] header = new byte[8];
        raf.readFully(header);
        int len1 = Integer.parseInt(new String(header, StandardCharsets.UTF_8));
        // 20 bytes of first header , len1 of first json body , 1 byte of \n , 20 bytes of second header
        // after reading these many bytes we will have next json record
        long record2JsonStarting = 20 + len1 + 1 + 20;
        raf.seek(record2JsonStarting + 3);
        return raf;
       
    }

}
