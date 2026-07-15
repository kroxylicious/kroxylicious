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

import io.kroxylicious.it.testplugins.PassThroughRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for cluster definition changes via hot reload. Each test
 * exercises {@code KafkaProxy#reconfigure(Configuration)} with a delta that mutates a
 * cluster definition, then asserts on the observable lifecycle effects via
 * {@link InvocationCountingFilterFactory}'s per-UUID initialize/close counters.
 * <p>
 * A cluster definition change is detected by {@code ClusterDefinitionChangeDetector} as a
 * virtual cluster {@code modify}, planned as a {@code ReplaceCluster} operation, and executed
 * as {@code RemoveCluster + AddCluster}. The affected VC's filter chain is torn down and
 * rebuilt, producing an observable increment in the filter's initialize and close counters.
 */
class ClusterDefinitionChangeHotReloadIT extends BaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterDefinitionChangeHotReloadIT.class);

    private static final int PORT_BLOCK_BASE = 23000 + ThreadLocalRandom.current().nextInt(2000);
    // Hot-reload tests reconfigure a running proxy, so the proxy must bind to a known port before
    // and after reconfiguration. Most ITs let the framework allocate ports, but here each test
    // scenario needs its own fixed port so concurrent test forks don't collide. Each block is
    // spaced 100 ports apart to leave room for multi-broker topologies within a scenario.
    private static final int PORT_CLUSTER_DEF_CHANGE = PORT_BLOCK_BASE;
    private static final int PORT_CROSS_VC_A = PORT_BLOCK_BASE + 100;
    private static final int PORT_CROSS_VC_B = PORT_BLOCK_BASE + 110;
    private static final int PORT_ROUTER_VIA_CLUSTER = PORT_BLOCK_BASE + 200;

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "backing-route";

    private static final Duration RECONFIGURE_TIMEOUT = Duration.ofSeconds(15);

    static {
        LoggerFactory.getLogger(ClusterDefinitionChangeHotReloadIT.class)
                .atInfo()
                .addKeyValue("portBlockBase", PORT_BLOCK_BASE)
                .log("ClusterDefinitionChangeHotReloadIT: per-JVM port block base chosen");
    }

    @AfterEach
    void afterEach() {
        InvocationCountingFilterFactory.assertAllClosedAndResetCounts();
    }

    @Test
    void shouldRestartVcWhenNamedClusterDefinitionChanges(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Changing a cluster definition's bootstrapServers triggers ClusterDefinitionChangeDetector
        // to flag the referencing VC as modified. ReplaceCluster tears down and rebuilds the VC,
        // which is observable via the filter's initialize/close counts.
        UUID filterId = UUID.randomUUID();
        var filterDef = invocationCounterDef("counter", filterId);

        var clusterDefV1 = new ClusterDefinition("upstream", cluster.getBootstrapServers(), null);
        // Duplicate the bootstrap entry — different string, same connectivity.
        var clusterDefV2 = new ClusterDefinition("upstream", cluster.getBootstrapServers() + "," + cluster.getBootstrapServers(), null);

        var vc = namedClusterVc("vc-cluster-change", PORT_CLUSTER_DEF_CHANGE, "upstream", "counter");

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterDef)
                .addToClusterDefinitions(clusterDefV1)
                .addToVirtualClusters(vc);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterDef)
                .addToClusterDefinitions(clusterDefV2)
                .addToVirtualClusters(vc)
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                .createDefaultKroxyliciousTester()) {

            // Given: VC serving traffic via a named cluster definition
            String topic = tester.createTopic("vc-cluster-change");
            assertProduceConsumeRoundTrip(tester, "vc-cluster-change", topic, "before-reconfigure");

            // When: the cluster definition's bootstrapServers changes
            LOGGER.info("Reconfiguring: updating cluster definition bootstrapServers");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean cluster definition update")
                            .isFalse());

            // Then: the VC was restarted — filter was initialized at startup and again after the
            // cluster definition change; the old init result was closed once by ReplaceCluster.
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filterId))
                    .as("filter should have been initialized at startup and once more after the cluster def change")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filterId))
                    .as("old filter init result should have been closed exactly once by ReplaceCluster")
                    .isEqualTo(1);

            // Then: traffic still flows after the reload
            assertProduceConsumeRoundTrip(tester, "vc-cluster-change", topic, "after-reconfigure");
        }
    }

    @Test
    void shouldNotRestartUnaffectedVcWhenDifferentClusterDefinitionChanges(
                                                                           @BrokerCluster KafkaCluster cluster)
            throws Exception {
        // Per-VC isolation: changing cluster-a's definition restarts vc-a but leaves vc-b untouched,
        // even though both use the same underlying Kafka cluster.
        UUID filterAId = UUID.randomUUID();
        UUID filterBId = UUID.randomUUID();
        var filterADef = invocationCounterDef("counter-a", filterAId);
        var filterBDef = invocationCounterDef("counter-b", filterBId);

        var clusterDefAv1 = new ClusterDefinition("cluster-a", cluster.getBootstrapServers(), null);
        var clusterDefAv2 = new ClusterDefinition("cluster-a", cluster.getBootstrapServers() + "," + cluster.getBootstrapServers(), null);
        var clusterDefB = new ClusterDefinition("cluster-b", cluster.getBootstrapServers(), null);

        var vcA = namedClusterVc("vc-a", PORT_CROSS_VC_A, "cluster-a", "counter-a");
        var vcB = namedClusterVc("vc-b", PORT_CROSS_VC_B, "cluster-b", "counter-b");

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterADef)
                .addToFilterDefinitions(filterBDef)
                .addToClusterDefinitions(clusterDefAv1)
                .addToClusterDefinitions(clusterDefB)
                .addToVirtualClusters(vcA, vcB);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterADef)
                .addToFilterDefinitions(filterBDef)
                .addToClusterDefinitions(clusterDefAv2)
                .addToClusterDefinitions(clusterDefB)
                .addToVirtualClusters(vcA, vcB)
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                .createDefaultKroxyliciousTester()) {

            // Given: both VCs serving traffic
            String topicA = tester.createTopic("vc-a");
            String topicB = tester.createTopic("vc-b");
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "before-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "before-vc-b");
            int vcBInitBefore = InvocationCountingFilterFactory.initializationCountFor(filterBId);
            int vcBCloseBefore = InvocationCountingFilterFactory.closeCountFor(filterBId);

            // When: only cluster-a's definition changes; cluster-b's is identical
            LOGGER.info("Reconfiguring: updating cluster-a definition only");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors()).isFalse());

            // Then: vc-a's filter was restarted
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filterAId))
                    .as("vc-a filter should have been initialized twice (startup + after cluster-a change)")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filterAId))
                    .as("vc-a old filter init result should have been closed once by ReplaceCluster")
                    .isEqualTo(1);

            // Then: vc-b's filter was not touched — per-VC isolation holds
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filterBId))
                    .as("vc-b's filter must not be re-initialized when only cluster-a's definition changes")
                    .isEqualTo(vcBInitBefore);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filterBId))
                    .as("vc-b's filter must not be closed when only cluster-a's definition changes")
                    .isEqualTo(vcBCloseBefore);

            // Then: both VCs still serve traffic
            assertProduceConsumeRoundTrip(tester, "vc-a", topicA, "after-vc-a");
            assertProduceConsumeRoundTrip(tester, "vc-b", topicB, "after-vc-b");
        }
    }

    @Test
    void shouldRestartVcWhenClusterDefinitionChangedViaRouter(@BrokerCluster KafkaCluster cluster) throws Exception {
        // Changing a cluster definition's bootstrapServers triggers ClusterDefinitionChangeDetector
        // to flag VCs that reach the definition through a router, not just direct references.
        UUID filterId = UUID.randomUUID();
        var filterDef = invocationCounterDef("counter", filterId);

        var clusterDefV1 = new ClusterDefinition("upstream", cluster.getBootstrapServers(), null);
        var clusterDefV2 = new ClusterDefinition("upstream", cluster.getBootstrapServers() + "," + cluster.getBootstrapServers(), null);

        var routerDef = passThroughRouterDef("my-router", "upstream");
        var vc = routerVc("vc-via-router", PORT_ROUTER_VIA_CLUSTER, "my-router", "counter");

        var startingBuilder = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterDef)
                .addToClusterDefinitions(clusterDefV1)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);

        var afterConfig = KroxyliciousConfigUtils.baseConfigurationBuilder()
                .addToFilterDefinitions(filterDef)
                .addToClusterDefinitions(clusterDefV2)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc)
                .build();

        try (KroxyliciousTester tester = KroxyliciousTesters.newBuilder(startingBuilder)
                .setFeatures(ROUTING_ENABLED)
                .createDefaultKroxyliciousTester()) {

            // Given: VC serving traffic via a router that targets the cluster definition
            String topic = tester.createTopic("vc-via-router");
            assertProduceConsumeRoundTrip(tester, "vc-via-router", topic, "before-reconfigure");

            // When: the cluster definition's bootstrapServers changes
            LOGGER.info("Reconfiguring: updating cluster definition bootstrapServers (router path)");
            assertThat(tester.reconfigure(afterConfig))
                    .succeedsWithin(RECONFIGURE_TIMEOUT)
                    .satisfies(rr -> assertThat(rr.hasErrors())
                            .as("ReconfigureResult should have no errors for a clean cluster definition update via router")
                            .isFalse());

            // Then: the VC was restarted — filter initialized at startup and again after the change
            assertThat(InvocationCountingFilterFactory.initializationCountFor(filterId))
                    .as("filter should have been initialized at startup and once more after the cluster def change via router")
                    .isEqualTo(2);
            assertThat(InvocationCountingFilterFactory.closeCountFor(filterId))
                    .as("old filter init result should have been closed exactly once by ReplaceCluster")
                    .isEqualTo(1);

            // Then: traffic still flows after the reload
            assertProduceConsumeRoundTrip(tester, "vc-via-router", topic, "after-reconfigure");
        }
    }

    // ---- fixture helpers ----

    private static VirtualCluster namedClusterVc(String name, int port, String clusterDefName, String... filterNames) {
        return new io.kroxylicious.proxy.config.VirtualClusterBuilder()
                .withName(name)
                .withTarget(new RouteTarget(clusterDefName, null))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .addToFilters(filterNames)
                .build();
    }

    private static NamedFilterDefinition invocationCounterDef(String name, UUID uuid) {
        return new NamedFilterDefinitionBuilder(name, InvocationCountingFilterFactory.class.getSimpleName())
                .withConfig("configInstanceId", uuid)
                .build();
    }

    private static RouterDefinition passThroughRouterDef(String name, String clusterDefName) {
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(clusterDefName, null));
        var routerConfig = new PassThroughRouterFactory.Config(ROUTE_NAME);
        return new RouterDefinition(name, PassThroughRouterFactory.class.getName(), routerConfig, List.of(route));
    }

    private static VirtualCluster routerVc(String name, int port, String routerName, String... filterNames) {
        return new VirtualClusterBuilder()
                .withName(name)
                .withTarget(new RouteTarget(null, routerName))
                .addToGateways(KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder(new HostPort("localhost", port)).build())
                .addToFilters(filterNames)
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
                ConsumerConfig.GROUP_ID_CONFIG, "cluster-def-change-it-" + vc + "-" + marker))) {
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
