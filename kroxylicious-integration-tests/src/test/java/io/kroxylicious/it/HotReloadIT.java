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
import org.junit.jupiter.api.parallel.ResourceLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.it.net.IntegrationTestInetAddressResolverProvider;
import io.kroxylicious.proxy.KafkaProxy;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.reload.ReconfigureError;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.integration.tester.SimpleMetricAssert;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.KeystoreManager;

import static io.kroxylicious.it.HotReloadIT.VcSlot.INCOMING;
import static io.kroxylicious.it.HotReloadIT.VcSlot.OUTGOING;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.DEFAULT_GATEWAY_NAME;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultSniHostIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end integration tests for {@link KafkaProxy#reconfigure(Configuration)}
 */
class HotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(HotReloadIT.class);

    private static final int PORT_REUSE_BOOTSTRAP = 9292;
    private static final String PORT_REUSE_LOCK = "localhost:" + PORT_REUSE_BOOTSTRAP;

    private static final String VC_BASELINE_NAME = "vc-baseline";
    private static final String VC_OUTGOING_NAME = "vc-outgoing";
    private static final String VC_INCOMING_NAME = "vc-incoming";

    private static final String SNI_BASE_DOMAIN = IntegrationTestInetAddressResolverProvider.generateFullyQualifiedDomainName(".hotreload");
    private static final String VC_BASELINE_BOOTSTRAP = "bootstrap-baseline" + SNI_BASE_DOMAIN + ":0";
    private static final String VC_OUTGOING_BOOTSTRAP = "bootstrap-outgoing" + SNI_BASE_DOMAIN + ":0";
    private static final String VC_INCOMING_BOOTSTRAP = "bootstrap-incoming" + SNI_BASE_DOMAIN + ":0";
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

            // Given
            LOGGER.info("producing + consuming through baseline + outgoing VCs");
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase1-baseline");
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, topic, "phase1-outgoing");

            // When
            LOGGER.info("reconfiguring to remove '{}'", VC_OUTGOING_NAME);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean pure-remove")
                            .isFalse());

            // Then
            LOGGER.info("verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase3-baseline");

            // new connections with vc-outgoing's SNI hostname are rejected. TLS
            // handshake succeeds (cert is wildcard, covers both hostnames), but the Kafka
            // session is closed because the VC behind that SNI binding is now STOPPED —
            // the SERVING-state guard rejects registration.
            LOGGER.info("verifying '{}' no longer accepts traffic on its SNI hostname", VC_OUTGOING_NAME);
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

            // Given
            LOGGER.info("producing + consuming through '{}' (only configured VC)", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase1-baseline");

            // When
            LOGGER.info("reconfiguring to add '{}'", VC_INCOMING_NAME);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean pure-add")
                            .isFalse());

            // Then
            LOGGER.info("verifying '{}' serves produce + consume after add", VC_INCOMING_NAME);
            String incomingTopic = tester.createTopic(VC_INCOMING_NAME);
            assertProduceConsumeRoundTrip(tester, VC_INCOMING_NAME, incomingTopic, "phase3-incoming");

            // vc-baseline remains undisturbed by the add.
            LOGGER.info("verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
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

            // Given
            LOGGER.info("producing + consuming through baseline + outgoing VCs");
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase1-baseline");
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, baselineTopic, "phase1-outgoing");

            // When
            LOGGER.info("reconfiguring to remove '{}' and add '{}' in one call", VC_OUTGOING_NAME, VC_INCOMING_NAME);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean mixed add+remove")
                            .isFalse());

            // Then
            LOGGER.info("verifying '{}' still serves produce + consume", VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, baselineTopic, "phase3-baseline");

            // vc-outgoing's SNI hostname no longer accepts traffic — the binding may
            // still be alive (shared acceptor channel) but the SERVING-state guard rejects
            // new connections because vc-outgoing is now STOPPED.
            LOGGER.info("verifying '{}' no longer accepts traffic on its SNI hostname", VC_OUTGOING_NAME);
            assertProducerFailure(tester, VC_OUTGOING_NAME, baselineTopic);

            // vc-incoming is reachable end-to-end.
            LOGGER.info("verifying '{}' serves produce + consume after the mixed reconfigure", VC_INCOMING_NAME);
            String incomingTopic = tester.createTopic(VC_INCOMING_NAME);
            assertProduceConsumeRoundTrip(tester, VC_INCOMING_NAME, incomingTopic, "phase5-incoming");
        }
    }

    /**
     * Port-addressed VCs exercise the per-VC acceptor channel rather than the shared acceptor
     * the SNI tests above use: each gateway binds its own port. On the remove side, the
     * interesting machinery is {@code EndpointRegistry.deregisterVirtualCluster} triggering a
     * {@code NetworkUnbindRequest} once the binding map empties for that port.
     */
    @Test
    void shouldReleasePortWhenPortAddressedVcIsRemoved(@BrokerCluster KafkaCluster cluster) throws Exception {
        var startingConfig = portConfig(
                portVc(cluster, "vc-retain"),
                portVc(cluster, "vc-release"));
        var afterConfig = portConfig(
                portVc(cluster, "vc-retain"));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            String retainTopic = tester.createTopic("vc-retain");
            String releaseTopic = tester.createTopic("vc-release");
            assertProduceConsumeRoundTrip(tester, "vc-retain", retainTopic, "given-retain");
            assertProduceConsumeRoundTrip(tester, "vc-release", releaseTopic, "given-release");
            int releasedPort = boundPort(tester, "vc-release");

            // When
            LOGGER.info("Reconfiguring to remove port-addressed VC bound to port {}", releasedPort);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean port-addressed remove")
                            .isFalse());

            // Then
            // NetworkUnbindRequest is queued on the binding-operation processor; the reconfigure
            // future completes when the unbind future does, but the OS-level socket release can
            // lag — assertPortIsBindable polls briefly.
            assertPortIsBindable(releasedPort);
            assertProduceConsumeRoundTrip(tester, "vc-retain", retainTopic, "then-retain");
        }
    }

    @Test
    void shouldStartServingAddedPortAddressedVcEndToEnd(@BrokerCluster KafkaCluster cluster) throws Exception {
        var startingConfig = portConfig(portVc(cluster, "vc-initial"));
        var afterConfig = portConfig(
                portVc(cluster, "vc-initial"),
                portVc(cluster, "vc-added"));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            String initialTopic = tester.createTopic("vc-initial");
            assertProduceConsumeRoundTrip(tester, "vc-initial", initialTopic, "given-initial");

            // When
            LOGGER.info("Reconfiguring to add port-addressed VC");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean port-addressed add")
                            .isFalse());

            // Then
            String addedTopic = tester.createTopic("vc-added");
            assertProduceConsumeRoundTrip(tester, "vc-added", addedTopic, "then-added");
            assertProduceConsumeRoundTrip(tester, "vc-initial", initialTopic, "then-initial");
        }
    }

    @Test
    void shouldSurfaceBindFailureAsReconfigureErrorWithoutBlockingOtherAdds(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Hold an OS-assigned port from outside the proxy so the proxy's bind attempt fails.
        try (var externalHolder = openSocketOnPort(0)) {
            int contestedPort = externalHolder.getLocalPort();

            var startingConfig = portConfig(portVc(cluster, "vc-initial"));
            var afterConfig = portConfig(
                    portVc(cluster, "vc-initial"),
                    portVc(cluster, "vc-good"),
                    portVc(cluster, "vc-blocked", contestedPort));

            var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
            try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

                // When
                LOGGER.info("Reconfiguring to add vc-good and vc-blocked (port {}, held externally)", contestedPort);
                assertThat(tester.reconfigure(afterConfig))
                        .succeedsWithin(RECONFIGURE_TIMEOUT)
                        .satisfies(rr -> {
                            assertThat(rr.hasErrors())
                                    .as("ReconfigureResult should report the bind failure for vc-blocked")
                                    .isTrue();
                            assertThat(rr.errors())
                                    .extracting(ReconfigureError::humanReadableIdentifier)
                                    .containsExactly("vc-blocked");
                        });

                // Then
                String goodTopic = tester.createTopic("vc-good");
                assertProduceConsumeRoundTrip(tester, "vc-good", goodTopic, "then-good");
            }
        }
    }

    @Test
    @ResourceLock(PORT_REUSE_LOCK)
    void shouldSupportPortReuseAcrossReconfigures(@BrokerCluster KafkaCluster cluster) throws Exception {
        var startingConfig = portConfig(
                portVc(cluster, "vc-retain"),
                portVc(cluster, "vc-original", PORT_REUSE_BOOTSTRAP));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            assertProduceConsumeRoundTrip(tester, "vc-original", tester.createTopic("vc-original"), "given-original");

            // When
            var afterRemove = portConfig(portVc(cluster, "vc-retain"));
            assertThat(tester.reconfigure(afterRemove))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Given
            assertPortIsBindable(PORT_REUSE_BOOTSTRAP);

            // When
            var afterReadd = portConfig(
                    portVc(cluster, "vc-retain"),
                    portVc(cluster, "vc-new", PORT_REUSE_BOOTSTRAP));
            assertThat(tester.reconfigure(afterReadd))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("Second reconfigure should bind vc-new cleanly on the freed port")
                            .isFalse());

            // Then
            String newTopic = tester.createTopic("vc-new");
            assertProduceConsumeRoundTrip(tester, "vc-new", newTopic, "then-new");
        }
    }

    @Test
    void shouldRemoveRuntimeAddedPortAddressedVc(@BrokerCluster KafkaCluster cluster) throws Exception {
        // When a VC is added at runtime and then removed in a subsequent
        // reconfigure, RemoveCluster must be able to resolve the original gateway via
        // VirtualClusterRegistry#virtualClusterModels.
        var startingConfig = portConfig(portVc(cluster, "vc-keep"));
        var afterAdd = portConfig(
                portVc(cluster, "vc-keep"),
                portVc(cluster, "vc-runtime-added"));
        var afterRemove = portConfig(portVc(cluster, "vc-keep"));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            String keepTopic = tester.createTopic("vc-keep");
            assertProduceConsumeRoundTrip(tester, "vc-keep", keepTopic, "given-keep");

            // When
            LOGGER.info("Reconfigure 1: adding vc-runtime-added");
            assertThat(tester.reconfigure(afterAdd))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("Reconfigure 1 (add) should have no errors")
                            .isFalse());

            String runtimeTopic = tester.createTopic("vc-runtime-added");
            assertProduceConsumeRoundTrip(tester, "vc-runtime-added", runtimeTopic, "given-runtime-added");
            int runtimeAddedPort = boundPort(tester, "vc-runtime-added");

            // When
            LOGGER.info("Reconfigure 2: removing the runtime-added vc-runtime-added");
            assertThat(tester.reconfigure(afterRemove))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("Reconfigure 2 (remove of runtime-added VC) should have no errors — "
                                    + "this is the regression: VirtualClusterRegistry must retain models for "
                                    + "runtime-added VCs so RemoveCluster can resolve their gateways")
                            .isFalse());

            // Then
            assertPortIsBindable(runtimeAddedPort);
            assertProduceConsumeRoundTrip(tester, "vc-keep", keepTopic, "then-keep");
        }
    }

    @Test
    void shouldModifyPortAddressedVcWithSamePort(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Same-port modify is the tight-timing case: ReplaceCluster's internal unbind happens
        // immediately before its rebind of the SAME port. Triggered here by flipping logNetwork
        // (a runtime-observable field whose change `VirtualCluster.sameAs` reports as a modify
        // but which doesn't affect client behaviour, so the cluster keeps working).
        //
        // Starts on port 0 so the OS assigns a port safely; after startup we capture the
        // bound port and reconfigure with that explicit port to guarantee same-port rebind.
        var startingConfig = portConfig(portVc(cluster, "vc-modify"));

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            String topic = tester.createTopic("vc-modify");
            assertProduceConsumeRoundTrip(tester, "vc-modify", topic, "given-pre-modify");
            int port = boundPort(tester, "vc-modify");

            // When
            var afterConfig = portConfig(portVcWithLogNetwork(cluster, "vc-modify", port, true));
            LOGGER.info("Reconfiguring to modify vc-modify (same port {}, logNetwork=true)", port);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean same-port modify")
                            .isFalse());

            // Then
            assertProduceConsumeRoundTrip(tester, "vc-modify", topic, "then-post-modify");
        }
    }

    @Test
    @ResourceLock(PORT_REUSE_LOCK)
    void shouldModifyPortAddressedVcWithDifferentPort(@BrokerCluster KafkaCluster cluster) throws Exception {
        int relocateFromPort = PORT_REUSE_BOOTSTRAP;
        int relocateToPort = PORT_REUSE_BOOTSTRAP + 10;
        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(portVc(cluster, "vc-relocate", relocateFromPort));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

            // Given
            String topicBefore = tester.createTopic("vc-relocate");
            assertProduceConsumeRoundTrip(tester, "vc-relocate", topicBefore, "given-old-port");

            // When
            var afterConfig = portConfig(portVc(cluster, "vc-relocate", relocateToPort));
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a port-relocation modify")
                            .isFalse());

            // Then
            tester.closeClientsFor("vc-relocate");
            assertPortIsBindable(relocateFromPort);
            String topicAfter = tester.createTopic("vc-relocate");
            assertProduceConsumeRoundTrip(tester, "vc-relocate", topicAfter, "then-new-port");
        }
    }

    @Test
    void shouldSurfaceModifyFailureAsReconfigureError(@BrokerCluster KafkaCluster cluster) throws Exception {
        // ReplaceCluster's internal add-half can fail (e.g. the new port is held externally).
        // The orchestrator surfaces this as a per-cluster ReconfigureError carrying the bind
        // cause — same shape as a pure-add bind failure. The cluster ends up offline.
        try (var externalHolder = openSocketOnPort(0)) {
            int contestedPort = externalHolder.getLocalPort();

            var startingConfig = portConfig(portVc(cluster, "vc-fail-modify"));
            var afterConfig = portConfig(portVc(cluster, "vc-fail-modify", contestedPort));

            var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                    .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
            try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder).createDefaultKroxyliciousTester()) {

                // Given
                int oldPort = boundPort(tester, "vc-fail-modify");

                // When
                LOGGER.info("Reconfiguring vc-fail-modify from port {} to port {} (held externally)", oldPort, contestedPort);
                assertThat(tester.reconfigure(afterConfig))
                        .succeedsWithin(RECONFIGURE_TIMEOUT)
                        .satisfies(rr -> {
                            assertThat(rr.hasErrors())
                                    .as("ReconfigureResult should report the bind failure on the new port")
                                    .isTrue();
                            assertThat(rr.errors())
                                    .extracting(ReconfigureError::humanReadableIdentifier)
                                    .containsExactly("vc-fail-modify");
                        });
                // Then
                assertPortIsBindable(oldPort);
            }
        }
    }

    @Test
    void shouldModifySniAddressedVcWithoutDisturbingOthers(@BrokerCluster KafkaCluster cluster) throws Exception {
        // SNI-addressed modify: both VCs share the proxy's SNI acceptor port. Flipping a
        // non-network field on one VC must not disturb the other.
        KeystoreTrustStorePair certs = buildKeystoreTrustStorePair("*" + SNI_BASE_DOMAIN);
        var baselineVc = buildSniVirtualCluster(cluster, certs, VC_BASELINE_NAME, VC_BASELINE_BOOTSTRAP, VC_BASELINE_BROKER_PATTERN);
        var modifyVcBefore = buildSniVirtualCluster(cluster, certs, VC_OUTGOING_NAME, VC_OUTGOING_BOOTSTRAP, VC_OUTGOING_BROKER_PATTERN);
        var modifyVcAfter = new VirtualClusterBuilder(modifyVcBefore).withLogNetwork(true).build();

        var startingConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(baselineVc, modifyVcBefore).build();
        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(baselineVc, modifyVcAfter).build();

        var testerBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToVirtualClusters(startingConfig.virtualClusters().toArray(new VirtualCluster[0]));
        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(testerBuilder)
                .setTrustStoreLocation(certs.clientTrustStore())
                .setTrustStorePassword(certs.password())
                .createDefaultKroxyliciousTester()) {

            // Given
            String topic = tester.createTopic(VC_BASELINE_NAME);
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase1-baseline");
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, topic, "phase1-modify");

            // When
            LOGGER.info("Reconfiguring to modify '{}' (flip logNetwork)", VC_OUTGOING_NAME);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean SNI modify")
                            .isFalse());

            // Then
            // Baseline is undisturbed.
            assertProduceConsumeRoundTrip(tester, VC_BASELINE_NAME, topic, "phase3-baseline");
            // Modified VC continues to serve.
            assertProduceConsumeRoundTrip(tester, VC_OUTGOING_NAME, topic, "phase3-modify");
        }
    }

    @Test
    void shouldExposeReconfigureAndLifecycleMetricsViaScrape(@BrokerCluster KafkaCluster cluster) {
        var startingConfig = portConfigBuilderWithMetrics(portVc(cluster, "vc-metrics-initial")).build();
        var afterConfig = new ConfigurationBuilder(startingConfig)
                .addToVirtualClusters(portVc(cluster, "vc-metrics-added"))
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(new ConfigurationBuilder(startingConfig)).createDefaultKroxyliciousTester();
                var management = tester.getManagementClient()) {

            // When
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Then
            await("reconfigure + lifecycle metrics exposed on /metrics")
                    .atMost(Duration.ofSeconds(10))
                    .pollInterval(Duration.ofMillis(200))
                    .untilAsserted(() -> {
                        var metrics = management.scrapeMetrics();
                        SimpleMetricAssert.assertThat(metrics)
                                .withUniqueMetric("kroxylicious_reconfigure_total", Map.of("outcome", "success"))
                                .value().isEqualTo(1.0);
                        SimpleMetricAssert.assertThat(metrics)
                                .withUniqueMetric("kroxylicious_reconfigure_clusters_affected_total",
                                        Map.of("operation", "add", "outcome", "success"))
                                .value().isEqualTo(1.0);
                        SimpleMetricAssert.assertThat(metrics)
                                .withUniqueMetric("kroxylicious_virtual_cluster_state",
                                        Map.of("virtual_cluster", "vc-metrics-added", "state", "serving"))
                                .value().isEqualTo(1.0);
                        SimpleMetricAssert.assertThat(metrics)
                                .withUniqueMetric("kroxylicious_virtual_cluster_transitions_total",
                                        Map.of("virtual_cluster", "vc-metrics-added", "from", "initializing", "to", "serving"))
                                .value().isEqualTo(1.0);
                    });
        }
    }

    private static ConfigurationBuilder portConfigBuilderWithMetrics(VirtualCluster... vcs) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .withNewManagement()
                .withNewEndpoints()
                .withNewPrometheus()
                .endPrometheus()
                .endEndpoints()
                .endManagement();
        for (var vc : vcs) {
            builder.addToVirtualClusters(vc);
        }
        return builder;
    }

    private static VirtualCluster portVc(KafkaCluster cluster, String name) {
        return portVc(cluster, name, 0);
    }

    private static VirtualCluster portVc(KafkaCluster cluster, String name, int port) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .build();
    }

    private static VirtualCluster portVcWithLogNetwork(KafkaCluster cluster, String name, int port, boolean logNetwork) {
        return KroxyliciousConfigUtils.baseVirtualClusterBuilder(cluster, name)
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .withLogNetwork(logNetwork)
                .build();
    }

    private static int boundPort(KroxyliciousTester tester, String vcName) {
        return HostPort.parse(tester.getBootstrapAddress(vcName, DEFAULT_GATEWAY_NAME)).port();
    }

    private static Configuration portConfig(VirtualCluster... vcs) {
        var builder = KroxyliciousConfigUtils.baseConfigurationBuilder();
        for (var vc : vcs) {
            builder.addToVirtualClusters(vc);
        }
        return builder.build();
    }

    /**
     * Polls until the port is bindable without SO_REUSEADDR. Using setReuseAddress(false)
     * ensures we wait until the OS has fully released the socket, not just until TIME_WAIT
     * allows a reuse-flagged bind to succeed.
     */
    private static void assertPortIsBindable(int port) {
        await("port " + port + " to be bindable")
                .atMost(Duration.ofSeconds(5))
                .pollInterval(Duration.ofMillis(100))
                .untilAsserted(() -> assertThatCode(() -> {
                    try (var s = new ServerSocket()) {
                        s.setReuseAddress(false);
                        s.bind(new InetSocketAddress((InetAddress) null, port));
                    }
                }).doesNotThrowAnyException());
    }

    /**
     * Exclusively bind on {@code port} so the proxy's bind attempt at
     * the same port fails.
     */
    private static ServerSocket openSocketOnPort(int port) {
        ServerSocket s = null;
        try {
            s = new ServerSocket();
            s.setReuseAddress(true);
            s.bind(new InetSocketAddress((InetAddress) null, port));
            return s;
        }
        catch (IOException e) {
            // bind() can throw after the socket was constructed — close it so the FD
            // isn't leaked and the OS releases any half-claimed port.
            if (s != null) {
                try {
                    s.close();
                }
                catch (IOException closeError) {
                    e.addSuppressed(closeError);
                }
            }
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
