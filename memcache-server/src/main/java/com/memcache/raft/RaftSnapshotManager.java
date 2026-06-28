package com.memcache.raft;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
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

    public boolean serialize(Map<String,CacheItem> cacheState, int lastIncludedIndex, int lastIncludedTerm, String nodeId){
        String tempFolderPath = "tmp/"+nodeId;
        String destFolderPath = "snapshots/"+nodeId;
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
            
            objectMapper.writeValue(bw, snapshot);
            bw.flush();
            fos.getFD().sync();

            try{
                Files.move(Path.of(tempFilePath), Path.of(destFilepath), StandardCopyOption.ATOMIC_MOVE);
                return true;
            }catch(IOException e){
                // here we can get IOexception if the dest file path and temp file path are of different FS.
                LOGGER.info("error while renaming file please check stack trace ", e);
            }

        }catch (Exception e) {
            LOGGER.error("snapshotting process failed please look at stack trace",e);
        }

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
        String destPath = "snapshots/"+nodeId+"/snapshot.snap";
        RaftSnapshot raftSnapShot = null;
        boolean isPresent = Files.isReadable(Path.of(destPath));
        if(!isPresent) return null;
        File file  = new File(destPath);
        try{
            raftSnapShot = objectMapper.readValue(file, RaftSnapshot.class);
        }catch(Exception e){
            LOGGER.error("exception while reading snapshot , pleaes check stack trace ",e);
        }
        return raftSnapShot;
    }

}