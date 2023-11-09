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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.systemtests.executor.Exec;
import io.kroxylicious.systemtests.templates.kroxylicious.KroxyConfigTemplates;

import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

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
        Path path = Path.of(System.getProperty("user.dir")).getParent();
        File dir = new File(path.toString() + File.separator + "kroxylicious-app/target");
        AtomicReference<Path> kroxyStart = new AtomicReference<>();
        File file;
        try (Stream<Path> walkStream = Files.walk(dir.toPath())) {
            walkStream.filter(p -> p.toFile().isFile()).forEach(f -> {
                if (f.toString().endsWith("kroxylicious-start.sh")) {
                    kroxyStart.set(f);
                }
            });
            file = File.createTempFile("config", ".yaml");
            Files.writeString(file.toPath(), KroxyConfigTemplates.getDefaultExternalKroxyConfigMap(clusterIp));
            file.deleteOnExit();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        pid = Exec.execWithoutWait(kroxyStart.toString(), "-c", file.getAbsolutePath());
        while (!thread.isInterrupted()) {
            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
            throw new RuntimeException(e);
        }
        String bootstrap = clusterIP + ":9292";
        LOGGER.debug("Kroxylicious bootstrap: " + bootstrap);
        return bootstrap;
    }

    /**
     * Is running boolean.
     *
     * @return the boolean
     */
    public boolean isRunning() {
        if (thread.isAlive()) {
            await().atMost(3, TimeUnit.SECONDS).until(() -> ProcessHandle.of(pid).isPresent());
        }
        return thread.isAlive() && ProcessHandle.of(pid).isPresent();
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
