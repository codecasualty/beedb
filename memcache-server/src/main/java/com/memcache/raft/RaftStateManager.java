package com.memcache.raft;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
/*
JSON format
{
    "term":1,
    "votedFor":"node1"
} */

public class RaftStateManager {

    private final Logger       LOGGER       = LoggerFactory.getLogger(RaftStateManager.class.getName());
    private final ObjectMapper objectMapper = new ObjectMapper();
    private       String       STATE_DIR    = "state";
    private       String       TMP_DIR      = "tmp";

    public RaftStateManager(String stateDir, String tmpDir){
        this.STATE_DIR = stateDir;
        this.TMP_DIR = tmpDir;
    }

    public boolean serialize(int currentTerm , String votedFor, String nodeId){
        String tempFolderPath = TMP_DIR+"/"+nodeId;
        String destFolderPath = STATE_DIR+"/"+nodeId;
        String tempFilePath = tempFolderPath+"/state.tmp";
        String destFilePath = destFolderPath+"/state.snap";

        boolean isTempDirectoryExist = createDirectoryWithPermissions(tempFolderPath);
        boolean isTargetDirectoryExist = createDirectoryWithPermissions(destFolderPath);

        RaftState raftState = new RaftState(currentTerm, votedFor);
        if(!isTargetDirectoryExist || !isTempDirectoryExist) return false;
        LOGGER.info("serializing raft state to file {} ", destFilePath);
        LOGGER.info("raft state : {} ", raftState);
        try(
            FileOutputStream fos = new FileOutputStream(tempFilePath);
            OutputStreamWriter osw = new OutputStreamWriter(fos,  StandardCharsets.UTF_8);
            BufferedWriter bw = new BufferedWriter(osw);
        ){
            /*
            we haven't use object mapeper.writevalue() because it creates a jsongenerator wrapping bw , writes the values and then closes the generator
            and by default jsongenerator.feature.auto close target is true, which means closing the generator will close the target
            */
            String json = objectMapper.writeValueAsString(raftState);
            bw.write(json);
            bw.flush();
            fos.getFD().sync();
            try{
                Files.move(Path.of(tempFilePath) , Path.of(destFilePath), StandardCopyOption.ATOMIC_MOVE);
                return true;
            }catch(IOException e){
                LOGGER.error("error while renaming fies , in raftstate ", e);
            }
        }catch(Exception e){
            LOGGER.error("Snapshotting raft sttae cause errors, check stack trace ", e);
        }
        return false;
    }   

    public boolean createDirectoryWithPermissions(String dirPath){
        try {
            Files.createDirectories(Path.of(dirPath));
            java.util.Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxr-xr-x");
            Files.setPosixFilePermissions(Path.of(dirPath), permissions);
            return true;
        } catch (Exception e) {
            LOGGER.error("Failed to create directory with permissions: " + dirPath, e);
            return false;
        }
    }

    public RaftState deserialize(String nodeId){
        String desPath = "state/"+nodeId+"/state.snap";
        RaftState raftState = null;
        boolean isPresent = Files.isReadable(Path.of(desPath));
        LOGGER.info("file {} is present {} ", desPath, isPresent);
        if(!isPresent) return raftState;
        File file = new File(desPath);
        try{
            raftState = objectMapper.readValue(file, RaftState.class);
        }catch(Exception e){
            LOGGER.error("exception while deserializing raft state ", e);
        }
        return raftState;
    }
}
