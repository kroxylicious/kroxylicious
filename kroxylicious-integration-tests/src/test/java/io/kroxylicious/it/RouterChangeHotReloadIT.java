/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.it;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.it.testplugins.InvocationCountingRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseVirtualClusterBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for router definition changes via hot reload. Each test exercises
 * {@code KafkaProxy#reconfigure(Configuration)} with a delta that mutates a router definition,
 * then asserts on the observable lifecycle effects via
 * {@link InvocationCountingRouterFactory}'s per-UUID initialize/close counters.
 * <p>
 * A router definition change is detected by {@code RouterChangeDetector} as a virtual cluster
 * {@code modify}, planned as a {@code ReplaceCluster} operation, and executed as
 * {@code RemoveCluster + AddCluster}. The affected VC's old {@code RouterChainFactory} is closed
 * (firing {@code RouterFactory.close()} on the old initResult) and a new one is built (firing
 * {@code RouterFactory.initialize()} with the updated config).
 */
class RouterChangeHotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RouterChangeHotReloadIT.class);

    private static final int PORT_BLOCK_BASE = 29000 + ThreadLocalRandom.current().nextInt(1000);
    private static final int PORT_ROUTER_CHANGE = PORT_BLOCK_BASE;
    private static final int PORT_CROSS_VC_A = PORT_BLOCK_BASE + 100;
    private static final int PORT_CROSS_VC_B = PORT_BLOCK_BASE + 110;
    private static final int PORT_ROUTING_DISABLED = PORT_BLOCK_BASE + 200;

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    private static final String ROUTE_NAME = "backing";
    private static final String CLUSTER_DEF_NAME = "backing";

    static {
        LoggerFactory.getLogger(RouterChangeHotReloadIT.class)
                .atInfo()
                .addKeyValue("portBlockBase", PORT_BLOCK_BASE)
                .log("RouterChangeHotReloadIT: per-JVM port block base chosen");
    }

    @AfterEach
    void afterEach() {
        InvocationCountingRouterFactory.assertAllClosedAndResetCounts();
    }

    @Test
    void shouldCloseOldRouterInitResultAndInitNewOneWhenRouterDefinitionChangesViaReload(
                                                                                         @BrokerCluster KafkaCluster cluster)
            throws Exception {
        // The central test for router-change hot reload: changing a router definition triggers
        // ReplaceCluster, which closes the old RouterChainFactory (firing close on the old
        // initResult) and builds a new one (firing initialize with the updated config).
        UUID oldConfigId = UUID.randomUUID();
        UUID newConfigId = UUID.randomUUID();

        var clusterDef = clusterDefinition(cluster);
        var vc = routerVc("vc-router-change", PORT_ROUTER_CHANGE, "my-router");

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef("my-router", oldConfigId))
                .addToVirtualClusters(vc);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef("my-router", newConfigId))
                .addToVirtualClusters(vc)
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                .setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester()) {

            // Given: VC serving traffic through the router with old config.
            String topic = tester.createTopic("vc-router-change");
            assertProduceConsumeRoundTrip(tester, "vc-router-change", topic, "before-reconfigure");

            // When: proxy reconfigured with the same router name but different config UUID.
            LOGGER.info("Reconfiguring vc-router-change: routerDef config {} -> {}", oldConfigId, newConfigId);
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean router-definition swap")
                            .isFalse());

            // Then: the old initResult was closed exactly once (RemoveCluster tore down the old
            // RouterChainFactory), and the new config was initialized exactly once (AddCluster
            // built a fresh RouterChainFactory).
            assertThat(InvocationCountingRouterFactory.closeCountFor(oldConfigId))
                    .as("old router's initResult should have been closed exactly once during ReplaceCluster")
                    .isEqualTo(1);
            assertThat(InvocationCountingRouterFactory.initializationCountFor(newConfigId))
                    .as("new router config should have been initialized exactly once")
                    .isEqualTo(1);
            assertThat(InvocationCountingRouterFactory.closeCountFor(newConfigId))
                    .as("new router's initResult is part of the live chain, not yet closed")
                    .isZero();

            // Then: real traffic confirms the VC is still serving correctly after the reload.
            assertProduceConsumeRoundTrip(tester, "vc-router-change", topic, "after-reconfigure");
        }
    }

    @Test
    void shouldNotAffectUnchangedVcRouterStateWhenAnotherVcsRouterDefinitionChanges(
                                                                                    @BrokerCluster KafkaCluster cluster)
            throws Exception {
        // Per-VC isolation: changing VC-A's router definition does not touch VC-B's router
        // state, even though both use the same router factory type.
        UUID vcAOldId = UUID.randomUUID();
        UUID vcANewId = UUID.randomUUID();
        UUID vcBId = UUID.randomUUID();

        var clusterDef = clusterDefinition(cluster);
        var vcA = routerVc("vc-a", PORT_CROSS_VC_A, "router-a");
        var vcB = routerVc("vc-b", PORT_CROSS_VC_B, "router-b");

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef("router-a", vcAOldId), routerDef("router-b", vcBId))
                .addToVirtualClusters(vcA, vcB);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToRouterDefinitions(routerDef("router-a", vcANewId), routerDef("router-b", vcBId))
                .addToVirtualClusters(vcA, vcB)
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                .setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester()) {

            // Given: both VCs serving traffic. Capture VC-B's counts before reconfigure so
            // the Then can assert "unchanged" via delta.
            String topicA = tester.createTopic("vc-a");
            String topicB = tester.createTopic("vc-b");
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "before-reconfigure-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "before-reconfigure-vc-b");
            int vcBInitBefore = InvocationCountingRouterFactory.initializationCountFor(vcBId);
            int vcBCloseBefore = InvocationCountingRouterFactory.closeCountFor(vcBId);

            // When: proxy reconfigured to change only VC-A's router definition; VC-B's is identical.
            LOGGER.info("Reconfiguring to change router-a only");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Then: VC-A's old router is closed and the new one is initialised.
            assertThat(InvocationCountingRouterFactory.closeCountFor(vcAOldId))
                    .as("VC-A's old router should have been closed exactly once")
                    .isEqualTo(1);
            assertThat(InvocationCountingRouterFactory.initializationCountFor(vcANewId))
                    .as("VC-A's new router should have been initialized exactly once")
                    .isEqualTo(1);

            // Then: VC-B's counts are unchanged — per-VC isolation holds.
            assertThat(InvocationCountingRouterFactory.initializationCountFor(vcBId))
                    .as("VC-B's router init count must not change during VC-A's reconfigure")
                    .isEqualTo(vcBInitBefore);
            assertThat(InvocationCountingRouterFactory.closeCountFor(vcBId))
                    .as("VC-B's router close count must not change during VC-A's reconfigure")
                    .isEqualTo(vcBCloseBefore);

            // Then: both VCs still serve traffic.
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "after-reconfigure-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "after-reconfigure-vc-b");
        }
    }

    @Test
    void shouldNotInitializeRouterWhenRoutingFeatureIsDisabledAndRouterDefinitionSubmittedViaHotReload(
                                                                                                       @BrokerCluster KafkaCluster cluster)
            throws Exception {
        // routerDefinitions is in the RECONCILABLE set, so the static-section differ permits
        // the change. RouterChangeDetector detects the new router, but since no VC references
        // it, no VC operations are planned and the reconfigure completes without errors.
        // The factory is never initialized.
        UUID routerId = UUID.randomUUID();

        var clusterDef = clusterDefinition(cluster);
        var vc = baseVirtualClusterBuilder(cluster, "vc-routing-disabled")
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", PORT_ROUTING_DISABLED)).build())
                .build();

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToVirtualClusters(vc);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToClusterDefinitions(clusterDef)
                .addToVirtualClusters(vc)
                .addToRouterDefinitions(routerDef("unused-router", routerId))
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                // Intentionally no setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester()) {

            // Given: proxy serving traffic via a plain VC (no router)
            String topic = tester.createTopic("vc-routing-disabled");
            assertProduceConsumeRoundTrip(tester, "vc-routing-disabled", topic, "before-reconfigure");

            // When: reconfigure submits routerDefinitions despite ROUTING being disabled
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("routerDefinitions change is reconcilable, no VC uses the router, so no errors expected")
                            .isFalse());

            // Then: the factory is never initialized — no VC references the router
            assertThat(InvocationCountingRouterFactory.initializationCountFor(routerId))
                    .as("factory must not be initialized when no VC references the router")
                    .isZero();

            // Then: plain VC continues serving traffic after reconfigure
            assertProduceConsumeRoundTrip(tester, "vc-routing-disabled", topic, "after-reconfigure");
        }
    }

    // ---- fixture helpers ----

    private static ClusterDefinition clusterDefinition(KafkaCluster cluster) {
        return new ClusterDefinition(CLUSTER_DEF_NAME, cluster.getBootstrapServers(), null);
    }

    private static RouterDefinition routerDef(String name, UUID configId) {
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(CLUSTER_DEF_NAME, null));
        var config = new InvocationCountingRouterFactory.Config(configId, ROUTE_NAME);
        return new RouterDefinition(name, InvocationCountingRouterFactory.class.getName(), config, List.of(route));
    }

    private static VirtualCluster routerVc(String name, int port, String routerName) {
        return new VirtualClusterBuilder()
                .withName(name)
                .withTarget(new RouteTarget(null, routerName))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .build();
    }

    private static void assertProduceConsumeRoundTrip(KroxyliciousTester tester, String vc, String topic,
                                                      String marker)
            throws Exception {
        int messageCount = 5;
        try (var producer = tester.producer(vc, Map.of(ProducerConfig.LINGER_MS_CONFIG, 0))) {
            for (int i = 0; i < messageCount; i++) {
                producer.send(new ProducerRecord<>(topic, marker + "-" + i, marker + "-v-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        try (var consumer = tester.consumer(vc, Map.of(
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_ID_CONFIG, "router-change-it-" + vc + "-" + marker))) {
            consumer.subscribe(List.of(topic));
            var seenKeys = new HashSet<String>();
            String keyPrefix = marker + "-";
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && seenKeys.size() < messageCount) {
                var batch = consumer.poll(Duration.ofMillis(500));
                for (var record : batch) {
                    if (record.key() != null && record.key().startsWith(keyPrefix)) {
                        seenKeys.add(record.key());
                    }
                }
            }
            assertThat(seenKeys)
                    .as("consumer should observe all %d records produced with marker '%s' via VC '%s'",
                            messageCount, marker, vc)
                    .hasSize(messageCount);
        }
    }
}
