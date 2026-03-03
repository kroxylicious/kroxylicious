/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigMapTemplates;
import io.kroxylicious.systemtests.utils.TestUtils;

import static org.awaitility.Awaitility.await;

/**
 * The type Kroxylicious app.
 */
public class KroxyliciousApp implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(KroxyliciousApp.class);
    private final String clusterIp;
    private final Thread thread;
    private long pid;

    /**
     * Instantiates a new Kroxylicious app.
     *
     * @param clusterIp the cluster ip
     */
    public KroxyliciousApp(String clusterIp) {
        this.clusterIp = clusterIp;
        thread = new Thread(this, "kroxy");
        thread.start();
    }

    public void run() {
        LOGGER.info("Launching kroxylicious app");
        Path parentPath = getProjectRoot();
        final Path targetPath = parentPath.resolve("kroxylicious-app").resolve("target");

        final Path startScript = resolveStartScript(targetPath);
        final Path configFile = generateKroxyliciousConfiguration();
        pid = Exec.execWithoutWait(startScript.toAbsolutePath().toString(), "-c", configFile.toAbsolutePath().toString());
    }

    private static Path getProjectRoot() {
        return Optional.ofNullable(System.getProperty("user.dir")).map(Path::of).map(Path::getParent)
                .orElseThrow(() -> new IllegalStateException("Unable to determine project root"));
    }

    private Path generateKroxyliciousConfiguration() {
        try {
            File configFile = Files.createTempFile("config", ".yaml", TestUtils.getDefaultPosixFilePermissions()).toFile();
            Files.writeString(configFile.toPath(), KroxyliciousConfigMapTemplates.getDefaultExternalKroxyliciousConfigMap(clusterIp));
            configFile.deleteOnExit();
            return configFile.toPath();
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to generate kroxylicious configuration file", e);
        }
    }

    private static Path resolveStartScript(Path targetPath) {
        try (Stream<Path> walkStream = Files.walk(targetPath)) {
            final Optional<Path> startScript = walkStream.filter(Files::isRegularFile).filter(f -> f.endsWith("kroxylicious-start.sh")).findFirst();
            if (startScript.isEmpty()) {
                throw new IllegalStateException("unable to find kroxylicious-start.sh");
            }
            else {
                return startScript.get();
            }
        }
        catch (IOException e) {
            throw new IllegalStateException("unable to find kroxylicious-start.sh", e);
        }
    }

    /**
     * Check if Kroxylicious process is running.
     *
     * @return the boolean
     */
    public boolean isRunning() {
        return ProcessHandle.of(pid).isPresent();
    }

    /**
     * Wait for kroxylicious process.
     */
    public void waitForKroxyliciousProcess() {
        await().atMost(5, TimeUnit.SECONDS).until(() -> ProcessHandle.of(pid).isPresent());
    }

    /**
     * Stop.
     */
    public void stop() {
        LOGGER.info("Stopping kroxylicious");
        thread.interrupt();
        ProcessHandle.of(pid).ifPresent(ProcessHandle::destroy);
    }
}
