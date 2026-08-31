package com.memcache.raft;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
// this class will be responsible for serializing and deserializing our snapshots
/*
JSON format
    {
      "lastAppliedIndex": 142,
      "lastAppliedTerm": 5,
      "cacheState": {
        "key1": {
          "flags": 0,
          "byteLength": 5,
          "expiresAt": 1782200000000,
          "infiniteExpiry": false,
          "value": "aGVsbG8="
        },
        "key2": {
          "flags": 0,
          "byteLength": 5,
          "expiresAt": 0,
          "infiniteExpiry": true,
          "value": "d29ybGQ="
        }
      }
    }
 */
public class RaftSnapshotManager {

    private final Logger       LOGGER       = LoggerFactory.getLogger(RaftSnapshotManager.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private       String       SNAPSHOT_DIR = "snapshots";
    private       String       TMP_DIR      = "tmp";
    private       String       SNAPSHOT_FILE_PATH = "snapshot.snap";
    public RaftSnapshotManager(String snapshotDir, String tmpDir, String nodeId){
        this.SNAPSHOT_DIR = snapshotDir;
        this.TMP_DIR = tmpDir;
        this.SNAPSHOT_FILE_PATH = SNAPSHOT_DIR+"/"+nodeId+"/snapshot.snap";
    }

    public boolean serialize(Map<String,CacheItem> cacheState, int lastIncludedIndex, long lastIncludedTerm, String nodeId){
        String tempFolderPath = TMP_DIR+"/"+nodeId;
        String destFolderPath = SNAPSHOT_DIR+"/"+nodeId;
        String tempFilePath = tempFolderPath+"/snapshot.tmp";
        String destFilepath = destFolderPath+"/snapshot.snap";
        boolean isTempDirectoryExist = createDirectoryWithPermissions(tempFolderPath);
        boolean isDestDirectoryExist = createDirectoryWithPermissions(destFolderPath);
        
        RaftSnapshot snapshot = new RaftSnapshot(lastIncludedIndex, lastIncludedTerm, cacheState);
        if(!isTempDirectoryExist || !isDestDirectoryExist) return false;


        try(
            FileOutputStream fos = new FileOutputStream(tempFilePath);
            OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            BufferedWriter bw = new BufferedWriter(osw);
        ){
            LOGGER.debug("we have opened files at {} and {} ", tempFilePath, destFilepath);
            String json = objectMapper.writeValueAsString(snapshot);
            bw.write(json);
            bw.flush();
            fos.getFD().sync();
            // below lines make sure parent directory metadata is updated
            LOGGER.debug("we have written json to file at {} ", tempFilePath);
            try{
                LOGGER.debug("moving file from {} to {}", tempFilePath, destFilepath);
                LOGGER.debug("snapshotted data is {} ", json);
                Files.move(Path.of(tempFilePath), Path.of(destFilepath), StandardCopyOption.ATOMIC_MOVE);
                try (FileChannel dir = FileChannel.open(Path.of(destFolderPath), StandardOpenOption.READ)) {
                    dir.force(true);
                }
                // /tmp/junit1685153358943795016/snapshots/node2/snapshot.snap 
                LOGGER.debug("snapshotting is done successfully and destFile path is {}", destFilepath);
                SNAPSHOT_FILE_PATH = destFilepath;
                return true;
            }catch(IOException e){
                // here we can get IOexception if the dest file path and temp file path are of different FS.
                LOGGER.debug("error while renaming file please check stack trace ", e);
            }

        }catch (Exception e) {
            LOGGER.error("snapshotting process failed please look at stack trace",e);
        }
        LOGGER.info("Serializing process is done successfully");
        return false;
    }

    public boolean createDirectoryWithPermissions(String dirPath){

        try{
            Files.createDirectories(Path.of(dirPath));
            Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxr-xr-x");
            Files.setPosixFilePermissions(Path.of(dirPath), permissions);
        }catch(IOException e){
            // its importatnt to note that we wont get IOexception while creating parent directories or giving them permissions
            // but lets say the dirPath has file in it or we dont have sufficient permissinos or fiel system is read only 
            LOGGER.error("error while directory creation , check stack trace ",e);
            return false;
        }
        return true;
    }

    public RaftSnapshot deserialize(String nodeId){
        // for deserilizing we will read the snapshot file and then use object mapper to convert it to our raftsnapshot
        String destPath = getDestPath(nodeId);
        LOGGER.debug("Deserialization in progresss from file {}", destPath);
        RaftSnapshot raftSnapShot = null;
        boolean isPresent = Files.isReadable(Path.of(destPath));
        LOGGER.debug("snpastho is present {}", isPresent);
        if(!isPresent) return null;
        File file  = new File(destPath);
        try{
            raftSnapShot = objectMapper.readValue(file, RaftSnapshot.class);
        }catch(Exception e){
            LOGGER.error("exception while reading snapshot , pleaes check stack trace ",e);
        }
        LOGGER.debug("raft snapshot is {} ", raftSnapShot);
        LOGGER.info("Deserializing process is done successfully");
        return raftSnapShot;
    }

    private String getDestPath(String nodeId){
        return SNAPSHOT_FILE_PATH;
    }

}