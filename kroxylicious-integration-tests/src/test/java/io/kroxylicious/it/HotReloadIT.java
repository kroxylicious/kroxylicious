/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.it.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.reload.ReconfigureError;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeystoreManager;

import static io.kroxylicious.it.HotReloadIT.VcSlot.INCOMING;
import static io.kroxylicious.it.HotReloadIT.VcSlot.OUTGOING;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * End-to-end integration tests for {@link KafkaProxy#reconfigure(Configuration)}
 */
class HotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotReloadIT.class);

    private static final String VC_BASELINE_NAME = "vc-baseline";
    private static final String VC_OUTGOING_NAME = "vc-outgoing";
    private static final String VC_INCOMING_NAME = "vc-incoming";

    private static final int SHARED_SNI_PORT = KroxyliciousConfigUtils.DEFAULT_PROXY_BOOTSTRAP.port();
    private static final String SNI_BASE_DOMAIN = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".hotreload");
    private static final String VC_BASELINE_BOOTSTRAP = "bootstrap-baseline" + SNI_BASE_DOMAIN + ":" + SHARED_SNI_PORT;
    private static final String VC_OUTGOING_BOOTSTRAP = "bootstrap-outgoing" + SNI_BASE_DOMAIN + ":" + SHARED_SNI_PORT;
    private static final String VC_INCOMING_BOOTSTRAP = "bootstrap-incoming" + SNI_BASE_DOMAIN + ":" + SHARED_SNI_PORT;
    private static final String VC_BASELINE_BROKER_PATTERN = "broker-$(nodeId)-baseline" + SNI_BASE_DOMAIN;
    private static final String VC_OUTGOING_BROKER_PATTERN = "broker-$(nodeId)-outgoing" + SNI_BASE_DOMAIN;
    private static final String VC_INCOMING_BROKER_PATTERN = "broker-$(nodeId)-incoming" + SNI_BASE_DOMAIN;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration PRODUCE_CONSUME_TIMEOUT = Duration.ofSeconds(15);
    private static final Duration REJECTION_TIMEOUT = Duration.ofSeconds(5);

    /**
     * Identifies a non-baseline VC slot used by the tests. {@link #buildConfig} takes a
     * varargs of these to declare which extras (beyond BASELINE) should appear in the
     * configuration being built.
     */
    enum VcSlot {
        OUTGOING(VC_OUTGOING_NAME, VC_OUTGOING_BOOTSTRAP, VC_OUTGOING_BROKER_PATTERN),
        INCOMING(VC_INCOMING_NAME, VC_INCOMING_BOOTSTRAP, VC_INCOMING_BROKER_PATTERN);

        final String name;
        final String bootstrap;
        final String brokerPattern;

        VcSlot(String name, String bootstrap, String brokerPattern) {
            this.name = name;
            this.bootstrap = bootstrap;
            this.brokerPattern = brokerPattern;
        }
    }

    @Test
    void shouldStopRemovedVcButContinueServingOthersEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Wildcard cert covering both VCs' SNI hostnames (both end in SNI_BASE_DOMAIN).
        KeystoreTrustStorePair certs = buildKeystoreTrustStorePair("*" + SNI_BASE_DOMAIN);
        var startingConfig = buildConfig(cluster, certs, OUTGOING); // baseline + outgoing
        var afterConfig = buildConfig(cluster, certs); // baseline only

        // Tester builder so we can register the truststore — required for the default client
        // configuration to do SSL handshakes against the SNI-addressed VCs.
        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder)
                .setTrustStoreLocation(certs.clientTrustStore())
                .setTrustStorePassword(certs.password())
                .createDefaultKroxyliciousTester()) {

            String topic = tester.createTopic(VC_BASELINE_NAME);

            // Phase 1: both SNI-addressed VCs serve produce + consume on the shared port.
            LOGGER.info("Phase 1: producing + consuming through baseline + outgoing VCs");
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase1-baseline");
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, topic, "phase1-outgoing");

            // Phase 2: reconfigure removes vc-outgoing. The acceptor channel stays alive
            // (vc-baseline still needs it); vc-outgoing transitions to STOPPED.
            LOGGER.info("Phase 2: reconfiguring to remove '{}'", VC_OUTGOING_NAME);
            var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean pure-remove")
                    .isFalse();

            // Phase 3: vc-baseline continues to serve on the same port + cert. An unaffected
            // VC is genuinely undisturbed by the reconfigure.
            LOGGER.info("Phase 3: verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase3-baseline");

            // Phase 4: new connections with vc-outgoing's SNI hostname are rejected. TLS
            // handshake succeeds (cert is wildcard, covers both hostnames), but the Kafka
            // session is closed because the VC behind that SNI binding is now STOPPED —
            // the SERVING-state guard rejects registration.
            LOGGER.info("Phase 4: verifying '{}' no longer accepts traffic on its SNI hostname", VC_OUTGOING_NAME);
            assertProducerFailure(tester, VC_OUTGOING_NAME, topic);
        }
    }

    @Test
    void shouldStartServingAddedVcEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Wildcard cert covering both VCs' SNI hostnames; required so the cert is already
        // valid for the to-be-added VC's hostname when registration runs at reconfigure time.
        KeystoreTrustStorePair certs = buildKeystoreTrustStorePair("*" + SNI_BASE_DOMAIN);
        var startingConfig = buildConfig(cluster, certs); // baseline only
        var afterConfig = buildConfig(cluster, certs, INCOMING); // baseline + incoming

        // Tester is built around the starting (one-VC) config. We pre-register the truststore
        // so client SSL handshakes against the SNI-addressed VCs succeed in every phase.
        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder)
                .setTrustStoreLocation(certs.clientTrustStore())
                .setTrustStorePassword(certs.password())
                .createDefaultKroxyliciousTester()) {

            String baselineTopic = tester.createTopic(VC_BASELINE_NAME);

            // Phase 1: only vc-baseline is configured and serving. vc-incoming is not yet known.
            LOGGER.info("Phase 1: producing + consuming through '{}' (only configured VC)", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase1-baseline");

            // Phase 2: reconfigure adds vc-incoming.
            LOGGER.info("Phase 2: reconfiguring to add '{}'", VC_INCOMING_NAME);
            var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean pure-add")
                    .isFalse();

            // Phase 3: the newly-added vc-incoming is reachable end-to-end.
            LOGGER.info("Phase 3: verifying '{}' serves produce + consume after add", VC_INCOMING_NAME);
            String incomingTopic = tester.createTopic(VC_INCOMING_NAME);
            assertProduceConsumeRoundTrip(tester, VC_INCOMING_NAME, incomingTopic, "phase3-incoming");

            // Phase 4: vc-baseline remains undisturbed by the add.
            LOGGER.info("Phase 4: verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase4-baseline");
        }
    }

    @Test
    void shouldHandleMixedAddAndRemoveInSingleReconfigure(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Wildcard cert covering every VC hostname in this test (baseline, outgoing, incoming).
        KeystoreTrustStorePair certs = buildKeystoreTrustStorePair("*" + SNI_BASE_DOMAIN);
        var startingConfig = buildConfig(cluster, certs, OUTGOING); // baseline + outgoing
        var afterConfig = buildConfig(cluster, certs, INCOMING); // baseline + incoming

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder)
                .setTrustStoreLocation(certs.clientTrustStore())
                .setTrustStorePassword(certs.password())
                .createDefaultKroxyliciousTester()) {

            String baselineTopic = tester.createTopic(VC_BASELINE_NAME);

            // Phase 1: starting state — baseline + outgoing both serve.
            LOGGER.info("Phase 1: producing + consuming through baseline + outgoing VCs");
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase1-baseline");
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, baselineTopic, "phase1-outgoing");

            // Phase 2: a single reconfigure both removes vc-outgoing AND adds vc-incoming.
            LOGGER.info("Phase 2: reconfiguring to remove '{}' and add '{}' in one call", VC_OUTGOING_NAME, VC_INCOMING_NAME);
            var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean mixed add+remove")
                    .isFalse();

            // Phase 3: vc-baseline is undisturbed by the mixed reconfigure.
            LOGGER.info("Phase 3: verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase3-baseline");

            // Phase 4: vc-outgoing's SNI hostname no longer accepts traffic — the binding may
            // still be alive (shared acceptor channel) but the SERVING-state guard rejects
            // new connections because vc-outgoing is now STOPPED.
            LOGGER.info("Phase 4: verifying '{}' no longer accepts traffic on its SNI hostname", VC_OUTGOING_NAME);
            assertProducerFailure(tester, VC_OUTGOING_NAME, baselineTopic);

            // Phase 5: vc-incoming is reachable end-to-end.
            LOGGER.info("Phase 5: verifying '{}' serves produce + consume after the mixed reconfigure", VC_INCOMING_NAME);
            String incomingTopic = tester.createTopic(VC_INCOMING_NAME);
            assertProduceConsumeRoundTrip(tester, VC_INCOMING_NAME, incomingTopic, "phase5-incoming");
        }
    }

    // -----------------------------------------------------------------------------------------
    // Port-addressed VCs. The SNI tests above share a single acceptor across multiple VCs;
    // these tests exercise the per-VC acceptor channel — each gateway binds its own port.
    // The interesting machinery on the remove side is endpointRegistry.deregisterVirtualCluster
    // triggering a NetworkUnbindRequest once the bindingMap empties for that port.
    // -----------------------------------------------------------------------------------------

    @Test
    void shouldReleasePortWhenPortAddressedVcIsRemoved(@BrokerCluster KafkaCluster cluster) throws Exception {
        int retainedPort = PORT_BLOCK_REMOVE;
        int releasedPort = PORT_BLOCK_REMOVE + PORT_STRIDE;

        var startingConfig = portConfig(cluster,
                portVc(cluster, "vc-retain", retainedPort),
                portVc(cluster, "vc-release", releasedPort));
        var afterConfig = portConfig(cluster,
                portVc(cluster, "vc-retain", retainedPort));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Both VCs serve initially — the phase-1 round-trips prove the proxy holds both
            // ports (clients couldn't connect otherwise).
            String retainTopic = tester.createTopic("vc-retain");
            String releaseTopic = tester.createTopic("vc-release");
            assertProduceConsumeRoundTrip(tester, "vc-retain", retainTopic, "phase1-retain");
            assertProduceConsumeRoundTrip(tester, "vc-release", releaseTopic, "phase1-release");

            LOGGER.info("Reconfiguring to remove port-addressed VC bound to port {}", releasedPort);
            var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean port-addressed remove")
                    .isFalse();

            // The released port is reclaimable from outside the proxy. NetworkUnbindRequest is
            // queued on the binding-operation processor; the reconfigure future completes when
            // the unbind future does, but the OS-level socket release can lag — poll briefly.
            assertPortIsBindable(releasedPort);

            // The retained VC is unaffected.
            assertProduceConsumeRoundTrip(tester, "vc-retain", retainTopic, "phase3-retain");
        }
    }

    @Test
    void shouldStartServingAddedPortAddressedVcEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        int initialPort = PORT_BLOCK_ADD;
        int addedPort = PORT_BLOCK_ADD + PORT_STRIDE;

        var startingConfig = portConfig(cluster, portVc(cluster, "vc-initial", initialPort));
        var afterConfig = portConfig(cluster,
                portVc(cluster, "vc-initial", initialPort),
                portVc(cluster, "vc-added", addedPort));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            String initialTopic = tester.createTopic("vc-initial");
            assertProduceConsumeRoundTrip(tester, "vc-initial", initialTopic, "phase1-initial");

            LOGGER.info("Reconfiguring to add port-addressed VC on port {}", addedPort);
            var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(result.hasErrors())
                    .as("ReconfigureResult should have no errors for a clean port-addressed add")
                    .isFalse();

            // New VC reachable end-to-end on its freshly bound port.
            String addedTopic = tester.createTopic("vc-added");
            assertProduceConsumeRoundTrip(tester, "vc-added", addedTopic, "phase3-added");

            // Existing VC undisturbed.
            assertProduceConsumeRoundTrip(tester, "vc-initial", initialTopic, "phase4-initial");
        }
    }

    @Test
    void shouldSurfaceBindFailureAsReconfigureErrorWithoutBlockingOtherAdds(@BrokerCluster KafkaCluster cluster) throws Exception {
        int initialPort = PORT_BLOCK_BINDFAIL;
        int goodPort = PORT_BLOCK_BINDFAIL + PORT_STRIDE;
        int contestedPort = PORT_BLOCK_BINDFAIL + 2 * PORT_STRIDE;

        // Hold the contested port from outside the proxy so the proxy's bind attempt fails.
        try (var externalHolder = openSocketOnPort(contestedPort)) {
            assertThat(externalHolder.getLocalPort()).isEqualTo(contestedPort);

            var startingConfig = portConfig(cluster, portVc(cluster, "vc-initial", initialPort));
            var afterConfig = portConfig(cluster,
                    portVc(cluster, "vc-initial", initialPort),
                    portVc(cluster, "vc-good", goodPort),
                    portVc(cluster, "vc-blocked", contestedPort));

            var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
            try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

                LOGGER.info("Reconfiguring to add vc-good (port {}) and vc-blocked (port {}, held externally)", goodPort, contestedPort);
                var result = tester.reconfigure(afterConfig).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);

                assertThat(result.hasErrors())
                        .as("ReconfigureResult should report the bind failure for vc-blocked")
                        .isTrue();
                assertThat(result.errors())
                        .extracting(ReconfigureError::humanReadableIdentifier)
                        .containsExactly("vc-blocked");

                // vc-good came up on its own port — a per-VC failure does not block other adds.
                String goodTopic = tester.createTopic("vc-good");
                assertProduceConsumeRoundTrip(tester, "vc-good", goodTopic, "phase3-good");
            }
        }
    }

    @Test
    void shouldSupportPortReuseAcrossReconfigures(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Proves the bind/unbind machinery composes: a port released by one remove is
        // available for a subsequent add in a later reconfigure.
        int retainedPort = PORT_BLOCK_REUSE;
        int reusedPort = PORT_BLOCK_REUSE + PORT_STRIDE;

        var startingConfig = portConfig(cluster,
                portVc(cluster, "vc-retain", retainedPort),
                portVc(cluster, "vc-original", reusedPort));
        var afterRemove = portConfig(cluster, portVc(cluster, "vc-retain", retainedPort));
        var afterReadd = portConfig(cluster,
                portVc(cluster, "vc-retain", retainedPort),
                portVc(cluster, "vc-new", reusedPort));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            assertProduceConsumeRoundTrip(tester, "vc-original", tester.createTopic("vc-original"), "phase1-original");

            LOGGER.info("First reconfigure: removing vc-original from port {}", reusedPort);
            var r1 = tester.reconfigure(afterRemove).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(r1.hasErrors()).isFalse();
            // We deliberately do NOT probe the freed port between the two reconfigures —
            // binding from the test would leave it in TIME_WAIT and prevent the proxy's
            // subsequent rebind. The second reconfigure's success IS the port-released proof.

            LOGGER.info("Second reconfigure: adding vc-new on the same port {}", reusedPort);
            var r2 = tester.reconfigure(afterReadd).get(RECONFIGURE_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            assertThat(r2.hasErrors())
                    .as("Second reconfigure should rebind cleanly on the freed port")
                    .isFalse();

            // The newly-added VC on the reused port is fully functional.
            String newTopic = tester.createTopic("vc-new");
            assertProduceConsumeRoundTrip(tester, "vc-new", newTopic, "phase3-new");
        }
    }

    private static VirtualCluster portVc(KafkaCluster cluster, String name, int port) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .build();
    }

    private static Configuration portConfig(KafkaCluster cluster, VirtualCluster... vcs) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder();
        for (var vc : vcs) {
            builder.addToVirtualClusters(vc);
        }
        return builder.build();
    }

    private static final int PORT_STRIDE = 10;
    private static final int PORT_BLOCK_REMOVE = 51000; // shouldReleasePortWhenPortAddressedVcIsRemoved
    private static final int PORT_BLOCK_ADD = 51100; // shouldStartServingAddedPortAddressedVcEndToEnd
    private static final int PORT_BLOCK_BINDFAIL = 51200; // shouldSurfaceBindFailureAsReconfigureError...
    private static final int PORT_BLOCK_REUSE = 51300; // shouldSupportPortReuseAcrossReconfigures

    /**
     * Asserts the port is bindable from outside the proxy within 5s.
     */
    private static void assertPortIsBindable(int port) throws InterruptedException {
        var deadline = System.currentTimeMillis() + 5_000;
        IOException lastError = null;
        while (System.currentTimeMillis() < deadline) {
            try (var s = new ServerSocket()) {
                s.bind(new InetSocketAddress((InetAddress) null, port));
                return;
            }
            catch (IOException e) {
                lastError = e;
                Thread.sleep(100);
            }
        }
        throw new AssertionError("port " + port + " was not released within 5s; last error: " + lastError);
    }

    /**
     * Exclusively bind on {@code port} so the proxy's bind attempt at
     * the same port fails.
     */
    private static ServerSocket openSocketOnPort(int port) {
        try {
            var s = new ServerSocket();
            s.bind(new InetSocketAddress((InetAddress) null, port));
            return s;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Build a {@link Configuration} containing {@code VC_BASELINE} plus the additional VCs
     * named by {@code extras}. Call sites read like declarations of intent:
     * <pre>{@code
     *   buildConfig(cluster, certs)                    // baseline only
     *   buildConfig(cluster, certs, OUTGOING)          // baseline + outgoing
     *   buildConfig(cluster, certs, INCOMING)          // baseline + incoming
     *   buildConfig(cluster, certs, OUTGOING, INCOMING) // baseline + both
     * }</pre>
     */
    private static Configuration buildConfig(KafkaCluster cluster, KeystoreTrustStorePair certs, VcSlot... extras) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(buildSniVirtualCluster(cluster, certs, VC_BASELINE_NAME, VC_BASELINE_BOOTSTRAP, VC_BASELINE_BROKER_PATTERN));
        for (var slot : extras) {
            builder.addToVirtualClusters(buildSniVirtualCluster(cluster, certs, slot.name, slot.bootstrap, slot.brokerPattern));
        }
        return builder.build();
    }

    private static VirtualCluster buildSniVirtualCluster(KafkaCluster cluster,
                                                         KeystoreTrustStorePair certs,
                                                         String name,
                                                         String bootstrap,
                                                         String brokerPattern) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultSniHostIdentifiesNodeGatewayBuilder(bootstrap, brokerPattern)
                        .withNewTls()
                        .withNewKeyStoreKey()
                        .withStoreFile(certs.brokerKeyStore())
                        .withNewInlinePasswordStoreProvider(certs.password())
                        .endKeyStoreKey()
                        .endTls()
                        .build())
                .build();
    }

    // Per-record produce: batch.size=1 + linger.ms=0 forces the producer to issue one
    // ProduceRequest per record rather than coalescing records into a single batched
    // request.
    private static final Map<String, Object> PER_RECORD_PRODUCER_CONFIG = Map.of(
            ProducerConfig.BATCH_SIZE_CONFIG, 1,
            ProducerConfig.LINGER_MS_CONFIG, 0,
            ProducerConfig.ACKS_CONFIG, "all");

    /**
     * Produce a random number of records (50-100) via {@code tester.producer(vc)}
     * configured with {@code batch.size=1, linger.ms=0} so each record results in its
     * own {@code ProduceRequest} — the proxy sees N round-trips rather than one batched
     * request. Then consume them back via {@code tester.consumer(vc)}. Records are keyed
     * {@code marker-0}..{@code marker-N-1} so the consumer can isolate this round-trip's
     * records from earlier phases' records that share the same topic.
     *
     * <p>Asserts the consumer observes exactly {@code N} unique keys carrying this call's
     * {@code marker} — proving the full proxy I/O round-trip is working and that no
     * records are dropped mid-stream across many independent produce requests.
     */
    private static void assertProduceConsumeRoundTrip(KroxyliciousTester tester, String vc, String topic, String marker) throws Exception {
        int messageCount = ThreadLocalRandom.current().nextInt(50, 101);
        LOGGER.info("Producing {} records via VC '{}' (marker='{}') with per-record requests", messageCount, vc, marker);

        var sendFutures = new ArrayList<Future<RecordMetadata>>(messageCount);
        try (var producer = tester.producer(vc, PER_RECORD_PRODUCER_CONFIG)) {
            for (int i = 0; i < messageCount; i++) {
                sendFutures.add(producer.send(new ProducerRecord<>(topic, marker + "-" + i, marker + "-value-" + i)));
            }
            for (Future<RecordMetadata> f : sendFutures) {
                f.get(PRODUCE_CONSUME_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            }
        }

        try (var consumer = tester.consumer(vc, Map.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_ID_CONFIG, "hotreload-it-consumer-" + marker))) {
            consumer.subscribe(List.of(topic));
            var seenKeys = new HashSet<String>();
            String keyPrefix = marker + "-";
            long deadline = System.currentTimeMillis() + PRODUCE_CONSUME_TIMEOUT.toMillis();
            while (System.currentTimeMillis() < deadline && seenKeys.size() < messageCount) {
                var batch = consumer.poll(Duration.ofMillis(500));
                for (var record : batch) {
                    if (record.key() != null && record.key().startsWith(keyPrefix)) {
                        seenKeys.add(record.key());
                    }
                }
            }
            assertThat(seenKeys)
                    .as("consumer should have read all %d records (marker='%s') via VC '%s' within %s",
                            messageCount, marker, vc, PRODUCE_CONSUME_TIMEOUT)
                    .hasSize(messageCount);
        }
    }

    /**
     * Open a fresh producer against the removed VC and verify it cannot deliver a record.
     * Uses aggressively short timeouts so the failure surfaces within
     * {@link #REJECTION_TIMEOUT} rather than the default Kafka client delivery timeout.
     */
    private static void assertProducerFailure(KroxyliciousTester tester, String vc, String topic) {
        try (var producer = tester.producer(vc, shortTimeoutProducerConfig())) {
            assertThatThrownBy(() -> producer.send(new ProducerRecord<>(topic, "should-fail", "should-fail-value"))
                    .get(REJECTION_TIMEOUT.toSeconds(), TimeUnit.SECONDS))
                    .as("producer should fail to deliver to the removed VC '%s'", vc)
                    .isInstanceOf(ExecutionException.class);
        }
    }

    private static Map<String, Object> shortTimeoutProducerConfig() {
        return Map.of(
                ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 3_000,
                ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 5_000,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 5_000,
                ProducerConfig.RETRIES_CONFIG, 1,
                ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, 100,
                ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG, 500);
    }

    private record KeystoreTrustStorePair(String brokerKeyStore, String clientTrustStore, String password) {}

    private static KeystoreTrustStorePair buildKeystoreTrustStorePair(String domain) throws Exception {
        var keystoreManager = new KeystoreManager();
        String dn = keystoreManager.buildDistinguishedName("test@kroxylicious.io", domain, "KI", "kroxylicious.io", null, null, "US");
        var bundle = keystoreManager.createSelfSignedCertificate(
                keystoreManager.newCertificateBuilder(dn));
        Path keystorePath = keystoreManager.generateCertificateFile(bundle);
        String password = keystoreManager.getPassword(keystorePath);
        // The generated JKS contains both the private key entry and the CA cert,
        // so the same file serves as both the proxy keystore and the client truststore.
        String keystore = keystorePath.toAbsolutePath().toString();
        return new KeystoreTrustStorePair(keystore, keystore, password);
    }
}
