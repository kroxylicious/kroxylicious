/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.installation.kroxylicious;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyliciousConfigTemplates;
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
        Path parentPath = Path.of(System.getProperty("user.dir")).getParent();
        final Path targetPath = parentPath.resolve("kroxylicious-app").resolve("target");

        final Path startScript = resolveStartScript(targetPath);
        final Path configFile = generateKroxyliciousConfiguration();
        pid = Exec.execWithoutWait(startScript.toAbsolutePath().toString(), "-c", configFile.toAbsolutePath().toString());
        while (!thread.isInterrupted()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private Path generateKroxyliciousConfiguration() {
        try {
            File configFile = Files.createTempFile("config", ".yaml", TestUtils.getDefaultPosixFilePermissions()).toFile();
            Files.writeString(configFile.toPath(), KroxyliciousConfigTemplates.getDefaultExternalKroxyConfigMap(clusterIp));
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
     * Gets bootstrap.
     *
     * @return the bootstrap
     */
    public String getBootstrap() {
        String clusterIP = null;
        try {
            var nis = NetworkInterface.getNetworkInterfaces();
            for (Iterator<NetworkInterface> it = nis.asIterator(); it.hasNext();) {
                var ni = it.next();
                for (Iterator<InetAddress> iter = ni.getInetAddresses().asIterator(); iter.hasNext();) {
                    var i = iter.next();
                    if (i.getHostAddress().startsWith("10")) {
                        clusterIP = i.getHostAddress();
                    }
                }
            }
        }
        catch (SocketException e) {
            throw new IllegalStateException("unable to determine bootstrap address", e);
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: {}", bootstrap);
        return bootstrap;
    }

    /**
     * Is running boolean.
     *
     * @return the boolean
     */
    public boolean isRunning() {
        return thread.isAlive() && ProcessHandle.of(pid).isPresent();
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
