/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connection accepted by the proxy but not yet in the {@code Forwarding} state (no Kafka handshake sent)
 * must complete its drain promise, and the channel should be closed.
 * <p>
 * This test simulates the leak scenario end-to-end: open a raw TCP socket that never sends
 * Kafka bytes (so the CCSM stays in {@code Startup}), trigger a {@code ReplaceCluster} via a
 * {@code logNetwork} toggle, and assert the proxy closes the socket. Repeated across cycles
 * to prove there is no connection leak.
 */
class ReconfigureConnectionLeakIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReconfigureConnectionLeakIT.class);

    // Fixed port so we can reconnect to the same address across reconfigure cycles.
    private static final int PORT_BLOCK_BASE = 31000 + ThreadLocalRandom.current().nextInt(1000);
    private static final int PORT_SOAK = PORT_BLOCK_BASE;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration SOCKET_CLOSE_TIMEOUT = Duration.ofSeconds(10);

    static {
        LoggerFactory.getLogger(ReconfigureConnectionLeakIT.class)
                .atInfo()
                .addKeyValue("portBlockBase", PORT_BLOCK_BASE)
                .log("ReconfigureConnectionLeakIT: per-JVM port block base chosen");
    }

    @Test
    void repeatedReconfiguresClosePreForwardingConnections(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Two configs that differ only in logNetwork — a genuine field-level change that
        // VirtualClusterChangeDetector reports as a modify, so ReplaceCluster fires and every
        // connection in the VC's activeConnections gets drained.
        var configA = portConfig(portVcWithLogNetwork(cluster, "vc-soak", PORT_SOAK, false));
        var configB = portConfig(portVcWithLogNetwork(cluster, "vc-soak", PORT_SOAK, true));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(configA.virtualClusters().toArray(new VirtualCluster[0]));

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            int cycles = 5;
            var currentConfig = configA;
            for (int cycle = 0; cycle < cycles; cycle++) {
                // Given — a raw TCP socket connected to the proxy that never sends a Kafka
                // handshake. The CCSM enters Startup and stays there; the initializer has
                // already called registerConnection so this connection is in the VC's
                // activeConnections set and will be seen by startDraining.
                Socket socket = new Socket();
                socket.connect(new InetSocketAddress("localhost", PORT_SOAK), 5000);
                assertThat(socket.isConnected()).as("cycle %d: raw socket should connect", cycle).isTrue();

                // When — reconfigure fires ReplaceCluster which drains every connection on
                // the old VC.
                var nextConfig = currentConfig == configA ? configB : configA;
                LOGGER.info("Cycle {}: toggling logNetwork to trigger ReplaceCluster", cycle);
                int finalCycle = cycle;
                assertThat(tester.reconfigure(nextConfig))
                        .succeedsWithin(RECONFIGURE_TIMEOUT)
                        .satisfies(rr -> assertThat(rr.hasErrors())
                                .as("cycle %d: reconfigure should complete without errors", finalCycle)
                                .isFalse());
                currentConfig = nextConfig;

                // Then — the proxy should have closed our socket. read() on an EOF socket
                // returns -1 immediately. If the fix regresses the socket stays open, in
                // which case read() blocks and eventually throws SocketTimeoutException;
                // we translate that to a sentinel value so the assertion message pinpoints
                // the failure mode.
                socket.setSoTimeout((int) SOCKET_CLOSE_TIMEOUT.toMillis());
                int result;
                try {
                    result = socket.getInputStream().read();
                }
                catch (SocketTimeoutException e) {
                    result = -2;
                }
                assertThat(result)
                        .as("cycle %d: proxy should have closed the pre-Forwarding socket (-1 = EOF, -2 = timeout / leak)", cycle)
                        .isEqualTo(-1);
                socket.close();
            }
        }
    }

    private static VirtualCluster portVcWithLogNetwork(KafkaCluster cluster, String name, int port, boolean logNetwork) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .withLogNetwork(logNetwork)
                .build();
    }

    private static Configuration portConfig(VirtualCluster... vcs) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder();
        for (var vc : vcs) {
            builder.addToVirtualClusters(vc);
        }
        return builder.build();
    }
}
