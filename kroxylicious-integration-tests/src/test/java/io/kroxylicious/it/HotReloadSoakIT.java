/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Tag;
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
 * Long-running soak test for hot-reload connection retention. Opens N persistent raw TCP
 * sockets (each in pre-Forwarding state — no Kafka bytes sent), loops M reconfigure cycles,
 * and samples heap usage across the loop. Prints old-gen occupancy after each sample so the
 * grow-or-flat pattern is observable in the console.
 * <p>
 * Intended for MANUAL execution to diagnose the CCSM {@code onDraining} pre-Forwarding leak.
 * Not part of the standard CI run — tagged {@code soak} so it's excluded by default.
 * <p>
 * <b>Expected behaviour</b>:
 * <ul>
 *   <li><b>With the fix</b> (CCSM.onDraining closes the channel on non-Forwarding drain):
 *       heap-after-GC stays flat across cycles. Old-gen occupancy at cycle N is roughly the
 *       same as at cycle 0.</li>
 *   <li><b>Without the fix</b>: heap grows linearly. Each reconfigure orphans N Netty
 *       channels; over hundreds of cycles this is hundreds of MB. Eventually GC pressure
 *       becomes visible and (with default heap) the JVM will start throwing
 *       OutOfMemoryError.</li>
 * </ul>
 * <p>
 * <b>A/B testing procedure</b>:
 * <ol>
 *   <li>Run with the fix in place — record the heap trend (should be flat).</li>
 *   <li>{@code git stash} the CCSM fix, rebuild, run again — record the heap trend
 *       (should grow linearly).</li>
 *   <li>{@code git stash pop} to restore.</li>
 * </ol>
 * <p>
 * <b>Configuration overrides</b> (system properties):
 * <ul>
 *   <li>{@code -Dsoak.cycles=500} — number of reconfigure cycles (default 200)</li>
 *   <li>{@code -Dsoak.connections=20} — persistent pre-Forwarding sockets (default 10)</li>
 *   <li>{@code -Dsoak.sampleEvery=25} — heap sample cadence in cycles (default 20)</li>
 * </ul>
 * <p>
 * <b>Invocation</b>:
 * <pre>
 * mvn test -pl kroxylicious-integration-tests -Dtest=HotReloadSoakIT -Dgroups=soak
 * </pre>
 */
@Tag("soak")
class HotReloadSoakIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotReloadSoakIT.class);

    private static final int PORT_BLOCK_BASE = 32000 + ThreadLocalRandom.current().nextInt(1000);
    private static final int PORT_SOAK = PORT_BLOCK_BASE;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(30);

    private static final int DEFAULT_CYCLES = Integer.getInteger("soak.cycles", 200);
    private static final int DEFAULT_CONNECTIONS = Integer.getInteger("soak.connections", 10);
    private static final int DEFAULT_SAMPLE_EVERY = Integer.getInteger("soak.sampleEvery", 20);

    @Test
    void soakHotReloadWithPersistentPreForwardingConnections(@BrokerCluster KafkaCluster cluster) throws Exception {
        LOGGER.atInfo()
                .addKeyValue("cycles", DEFAULT_CYCLES)
                .addKeyValue("connections", DEFAULT_CONNECTIONS)
                .addKeyValue("sampleEvery", DEFAULT_SAMPLE_EVERY)
                .addKeyValue("portBlockBase", PORT_BLOCK_BASE)
                .log("HotReloadSoakIT starting");

        // Two configs that alternate to force a real ReplaceCluster on every cycle.
        var configA = portConfig(portVcWithLogNetwork(cluster, "vc-soak", PORT_SOAK, false));
        var configB = portConfig(portVcWithLogNetwork(cluster, "vc-soak", PORT_SOAK, true));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(configA.virtualClusters().toArray(new VirtualCluster[0]));

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Open N persistent raw TCP sockets ONCE. These stay open across all reconfigure
            // cycles. Each sits in Startup state (no ApiVersions sent) so drain hits the
            // non-Forwarding branch of onDraining. Under the fix, each cycle's drain closes
            // these and we open fresh ones for the next cycle. Under the bug, each cycle
            // leaks all N channels.
            List<Socket> persistentSockets = new ArrayList<>();
            openPersistentSockets(persistentSockets, DEFAULT_CONNECTIONS);
            LOGGER.info("Opened {} persistent pre-Forwarding sockets", persistentSockets.size());

            // Baseline heap sample after everything is warm.
            printHeapSample("baseline", 0);

            var currentConfig = configA;
            for (int cycle = 1; cycle <= DEFAULT_CYCLES; cycle++) {
                var nextConfig = currentConfig == configA ? configB : configA;
                assertThat(tester.reconfigure(nextConfig))
                        .succeedsWithin(RECONFIGURE_TIMEOUT)
                        .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());
                currentConfig = nextConfig;

                // Sockets opened before this cycle would have been closed by the drain (with
                // the fix), so open fresh sockets to keep N pre-Forwarding connections attached
                // for the NEXT drain. Under the bug the previous sockets are still "connected"
                // from the proxy's perspective (leaked), and we're adding more leaks each cycle.
                closeSockets(persistentSockets);
                persistentSockets.clear();
                openPersistentSockets(persistentSockets, DEFAULT_CONNECTIONS);

                if (cycle % DEFAULT_SAMPLE_EVERY == 0) {
                    printHeapSample("after cycle " + cycle, cycle);
                }
            }

            printHeapSample("final", DEFAULT_CYCLES);
            closeSockets(persistentSockets);
        }
    }

    // ---- helpers ----

    private static void openPersistentSockets(List<Socket> destination, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            var socket = new Socket();
            socket.connect(new InetSocketAddress("localhost", PORT_SOAK), 5000);
            destination.add(socket);
        }
    }

    private static void closeSockets(List<Socket> sockets) {
        for (var s : sockets) {
            try {
                s.close();
            }
            catch (Exception ignored) {
                // Best-effort; some may already be closed by the proxy.
            }
        }
    }

    /**
     * Forces a full GC and prints heap usage. The forced GC ensures we're measuring
     * <em>reachable</em> memory, not GC-cycle noise — the linear-growth signal from the leak
     * is a growth in reachable objects (orphaned CCSMs holding orphaned pipelines), which
     * survives GC.
     */
    private static void printHeapSample(String label, int cycle) {
        System.gc(); // NOSONAR: soak test needs deterministic heap-usage samples
        try {
            Thread.sleep(200);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        LOGGER.atInfo()
                .addKeyValue("label", label)
                .addKeyValue("cycle", cycle)
                .addKeyValue("heapUsedMB", heap.getUsed() / (1024 * 1024))
                .addKeyValue("heapCommittedMB", heap.getCommitted() / (1024 * 1024))
                .addKeyValue("heapMaxMB", heap.getMax() / (1024 * 1024))
                .log("heap sample");
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
