/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.internal.DrainCoordinator;
import io.kroxylicious.proxy.internal.config.Features;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@Timeout(30)
class KafkaProxyShutdownOrderingTest {

    private final ConfigParser configParser = new ConfigParser();

    /**
     * Verifies that ports are unbound <em>before</em> drain completes.
     * <p>
     * With the correct ordering — {@code endpointRegistry.shutdown()} before
     * {@code drainAllClusters()} — no new connections can arrive after the drain
     * snapshot is taken, closing the race described in rodobario's review comment.
     * With the old (inverted) ordering, the port stays bound throughout drain,
     * allowing new connections to arrive after the snapshot.
     */
    @Test
    void portIsUnboundBeforeDrainCompletes() throws Exception {
        int proxyPort = freePort();

        var drainStarted = new CountDownLatch(1);
        var drainCanComplete = new CountDownLatch(1);

        var dc = new DrainCoordinator() {
            @Override
            public CompletableFuture<Void> drainCluster(String clusterName, Duration timeout) {
                drainStarted.countDown();
                return CompletableFuture.runAsync(() -> {
                    try {
                        drainCanComplete.await();
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        };

        var config = """
                virtualClusters:
                  - name: demo
                    drainTimeout: 10s
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:%d
                """.formatted(proxyPort);

        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures(), dc)) {
            proxy.startup();

            assertThat(canConnect(proxyPort))
                    .as("port should be reachable before shutdown")
                    .isTrue();

            var shutdownThread = new Thread(proxy::shutdown, "test-shutdown");
            shutdownThread.start();

            // Wait for drain to start — at this point endpointRegistry.shutdown() must
            // have already completed (with the fix) or not yet (with the old ordering).
            assertThat(drainStarted.await(10, TimeUnit.SECONDS))
                    .as("drain should have started")
                    .isTrue();

            // Port must be unreachable: endpointRegistry.shutdown() ran before drain.
            assertThatCode(() -> new Socket("localhost", proxyPort).close())
                    .as("proxy port should be unreachable while drain is in progress")
                    .isInstanceOf(Exception.class);

            drainCanComplete.countDown();
            shutdownThread.join(10_000);
            assertThat(shutdownThread.isAlive()).as("shutdown should have completed").isFalse();
        }
    }

    /**
     * Verifies that an exception during drain (e.g. a per-connection drain future completing
     * exceptionally) is caught inside {@code drainAllClusters()} and shutdown still completes
     * cleanly. Without this catch, a runaway drain failure would propagate out of shutdown
     * and the proxy would terminate abruptly without running the rest of its cleanup
     * (Netty {@code shutdownGracefully}, meter registry cleanup, lifecycle transitions).
     */
    @Test
    void drainFailureIsCaughtAndShutdownCompletes() throws Exception {
        int proxyPort = freePort();

        var dc = new DrainCoordinator() {
            @Override
            public CompletableFuture<Void> drainCluster(String clusterName, Duration timeout) {
                return CompletableFuture.failedFuture(new RuntimeException("simulated drain failure"));
            }
        };

        var config = """
                virtualClusters:
                  - name: demo
                    drainTimeout: 10s
                    targetCluster:
                      bootstrapServers: kafka.example:1234
                    gateways:
                    - name: default
                      portIdentifiesNode:
                        bootstrapAddress: localhost:%d
                """.formatted(proxyPort);

        try (var proxy = new KafkaProxy(configParser, configParser.parseConfiguration(config), Features.defaultFeatures(), dc)) {
            proxy.startup();

            // Shutdown should complete without throwing — the catch in drainAllClusters
            // swallows the drain failure and lets the rest of cleanup proceed.
            assertThatCode(proxy::shutdown).doesNotThrowAnyException();
        }
    }

    private static boolean canConnect(int port) {
        try (var ignored = new Socket()) {
            ignored.connect(new InetSocketAddress("localhost", port), 1000);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    private static int freePort() throws Exception {
        try (var s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }
}
