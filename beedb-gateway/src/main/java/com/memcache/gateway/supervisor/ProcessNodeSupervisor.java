package com.memcache.gateway.supervisor;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.memcache.gateway.chaos.chaosexception.NodeCouldNotBeKilled;

public class ProcessNodeSupervisor implements NodeSupervisor {  

    private final Set<String> nodeIds ;
    private final Path serverDirectory;
    private final int restartAfterSeconds;
    private static final int PKILL_TIMEOUT_SECONDS = 5;
    private final ThreadFactory chaosRestoreThreadFactory = runnable -> {
        Thread thread = new Thread(runnable, "chaos-restore");
        thread.setDaemon(true);
        return thread;
    };
    private final ScheduledExecutorService chaosRestorer = Executors.newScheduledThreadPool(1 , chaosRestoreThreadFactory);
    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessNodeSupervisor.class);
    public ProcessNodeSupervisor(Set<String> nodeIds, Path serverDirectory, int restartAfterSeconds){
        this.nodeIds = nodeIds;
        this.serverDirectory = serverDirectory;
        this.restartAfterSeconds = restartAfterSeconds;
    }

    @Override
    public void kill(String nodeId) throws NodeCouldNotBeKilled {
        if(nodeId == null || !nodeIds.contains(nodeId)) throw new NodeCouldNotBeKilled("node "+nodeId+" is not in the set of nodes to kill");
        String pattern = "Dnode\\.id="+nodeId+" -jar .*beedb-server.*\\.jar";
        try {
            Process process = new ProcessBuilder(List.of("pkill", "-f", pattern)).redirectErrorStream(true).start();
            if(!process.waitFor(PKILL_TIMEOUT_SECONDS, TimeUnit.SECONDS)){
                process.destroyForcibly();
                throw new NodeCouldNotBeKilled("pkill timed out for node "+nodeId+" "+new String(process.getInputStream().readAllBytes()));
            }
            int exitCode = process.exitValue();
            if(exitCode != 0){
                throw new NodeCouldNotBeKilled("pkill exited with code "+exitCode+" for node "+nodeId+" "+new String(process.getInputStream().readAllBytes()));
            }
            chaosRestorer.schedule(() -> {
                try {
                    chaosRestore(nodeId);
                } catch (Exception e) {
                    LOGGER.error("failed to restore node {}", nodeId, e);
                }
            }, restartAfterSeconds, TimeUnit.SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new NodeCouldNotBeKilled("Kill process was interrupted for node "+nodeId+" "+e.getMessage(),e);
        }catch (IOException e) {
            throw new NodeCouldNotBeKilled("Kill process failed for node "+nodeId+" "+e.getMessage(),e);
        }catch (RuntimeException e) {
            throw new NodeCouldNotBeKilled("Kill process failed for node "+nodeId+" "+e.getMessage(),e);
        }
        
    }

    private void chaosRestore(String nodeId) throws IOException{
        Path jar = serverJar();
        // stdout is discarded: logback's CONSOLE appender writes every log line there, and
        // logs/<node>.log already has them, rotated. stderr goes to a file instead it
        // stays empty unless the JVM dies of something only stderr sees, such as an
        // uncaught Error
        Path err = serverDirectory.resolve("logs").resolve(nodeId + ".err");
        Files.createDirectories(err.getParent());
        new ProcessBuilder(List.of(
        "setsid", "java", "-Dnode.id=" + nodeId,
        "-jar", jar.toString(), nodeId + ".properties"))
            .directory(serverDirectory.toFile())
            .redirectOutput(ProcessBuilder.Redirect.DISCARD)
            .redirectError(ProcessBuilder.Redirect.appendTo(err.toFile()))
            .start(); 
    }

    /**
     * the shaded jar in the server module's target/. found by glob rather than by a
     * hard-coded version, so a version bump in the pom does not silently stop chaos
     * from restoring nodes.
     */
    private Path serverJar() throws IOException {
        Path target = serverDirectory.resolve("target");
        try (DirectoryStream<Path> jars = Files.newDirectoryStream(target, "beedb-server-*.jar")) {
            for (Path jar : jars) {
                return jar;
            }
        }
        throw new IOException("no beedb-server jar in " + target.toAbsolutePath()
            + " -- run `make package` in the server module first");
    }
    
}
