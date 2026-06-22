package com.memcache.raft;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Base64;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;

import com.memcache.cache.Cache;
import com.memcache.cache.CacheItem;
// this class will be responsible for serializing and deserializing our snapshots 
public class RaftSnapshot {
    
    // the path where we will write our snapshot will be 
    // snapshots/<nodeId>/snapshot.snap
    // we will first write our snapshot in some temp file and then atomically rename it to correct path
    // so that we dont fall in some partial /corrupted state
    // serialization format 
    // header line :- lastIncludedIndex\tlastIncludedTerm
    // then one line per cache entry :- key\tbase64(value)\tflags\texpiresAt\tinfiniteExpiry

    private Logger LOGGER = LoggerFactory.getLogger(RaftSnapshot.class.getName());
    public boolean serialize(Cache cache, int lastIncludedIndex, int lastIncludedTerm, String nodeId){
        String tempFolderPath = "tmp/"+nodeId;
        String destFolderPath = "snapshots/"+nodeId;
        String tempFilePath = tempFolderPath+"/snapshot.tmp";
        String destFilepath = destFolderPath+"/snapshot.snap";
        boolean isTempDirectoryExist = createDirectoryWithPermissions(tempFolderPath);
        boolean isDestDirectoryExist = createDirectoryWithPermissions(destFolderPath);

        if(!isTempDirectoryExist || !isDestDirectoryExist) return false;
        
        try(
            FileOutputStream fos = new FileOutputStream(tempFilePath);
            OutputStreamWriter osw = new OutputStreamWriter(fos, StandardCharsets.UTF_8);
            BufferedWriter bw = new BufferedWriter(osw);
        ){
            // header line for
            bw.write(lastIncludedIndex+"\t"+lastIncludedTerm);
            bw.newLine();
            
            // i dont want to add serializations and desirealizations method in cache.java  file because its not its job 
            // for example in case of command class its supposed to be transported across nodes and network so serializations and deserializations there make
            // sense, but in case of cache, its our intrinsic DS , its raftsnapshot class concern like how it want to serialize it and how it want to deserialize it
            
            Set<String> keys = cache.getKeySet();
            for(String key : keys){
                CacheItem item = cache.get(key);
                // this can happen if the item has expired
                if(item == null) continue;
                byte[] value = item.getValue();
                long expiresAt = item.getExpiresAt();
                boolean infiniteExpiry = item.isInfiniteExpiry();
                int flags = item.getFlags();
                String encodedValue = value != null ? Base64.getEncoder().encodeToString(value) : "";
                bw.write(key+"\t");
                bw.write(encodedValue+"\t");
                bw.write(flags+"\t");
                bw.write(expiresAt+"\t");
                bw.write(infiniteExpiry+"");
                bw.newLine();
            }
            // as bufferwriter writes to OS page cache, not to disk to force it to write it to disk we need to do fsync()
            bw.flush();
            fos.getFD().sync();
            // now we have written all of data in serialized format in our temp file , now its time to atomically swap it
            try{
                Path returnedFilePath = Files.move(Path.of(tempFilePath), Path.of(destFilepath) , StandardCopyOption.ATOMIC_MOVE);
                return returnedFilePath.equals(Path.of(destFilepath));
            }catch(Exception e){
                LOGGER.error("exception during renaming file , check stack trace for details ",e);
            }
            
        }catch (Exception e) {
            // TODO: handle exception
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
            LOGGER.error("error while directory creation , check stack trace ",e);
            return false;
        }
        return true;
    }

}
