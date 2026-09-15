/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.router.TopologyCapturingRouterFactory;
import io.kroxylicious.kafka.common.Uuid;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests verifying that {@code TopologyService.topicNames()} resolves topic ids via a
 * real METADATA round-trip through a real backing Kafka cluster, and that
 * {@code TopologyService.invalidateRoute()} forces a fresh one.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class TopologyServiceIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();
    private static final String ROUTE_NAME = "backing-route";
    private static final String ROUTER_NAME = "topology-capturing";
    private static final String TARGET_CLUSTER_NAME = "backing";
    private static final String VC_NAME = "demo";

    @BeforeEach
    void resetRouterState() {
        TopologyCapturingRouterFactory.reset();
    }

    private ConfigurationBuilder topologyConfig(KafkaCluster cluster) {
        var targetCluster = new ClusterDefinition(TARGET_CLUSTER_NAME, cluster.getBootstrapServers(), null);
        var route = new RouteDefinition(ROUTE_NAME, 0, List.of(), new RouteTarget(TARGET_CLUSTER_NAME, null));
        var routerConfig = new TopologyCapturingRouterFactory.Config(ROUTE_NAME);
        var routerDef = new RouterDefinition(ROUTER_NAME,
                TopologyCapturingRouterFactory.class.getName(), routerConfig, List.of(route));
        var vc = new VirtualClusterBuilder()
                .withName(VC_NAME)
                .withTarget(new RouteTarget(null, ROUTER_NAME))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();
        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetCluster)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    /**
     * Converts a real broker-assigned topic id (as seen by the test's admin client) to the
     * proxy-internal {@link Uuid} type {@code TopologyService} deals in. Both types use the same
     * base64 string encoding, so round-tripping through the string form is a safe conversion.
     */
    private static Uuid toInternalUuid(org.apache.kafka.common.Uuid uuid) {
        return Uuid.fromString(uuid.toString());
    }

    @Test
    void shouldResolveTopicNameViaRealMetadataRoundTrip(KafkaCluster cluster) throws Exception {
        // Given
        var config = topologyConfig(cluster);
        var topicName = "topology-topic-names-" + UUID.randomUUID();

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin()) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var topicId = toInternalUuid(admin.describeTopics(List.of(topicName)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topicName).topicId());
            TopologyCapturingRouterFactory.topicIdsToResolve.set(Set.of(topicId));

            assertThat(TopologyCapturingRouterFactory.capturedTopicNames.get())
                    .as("Expect id resolution to be driven by admin.describeCluster()")
                    .doesNotContainEntry(topicId, topicName);

            // When: a DESCRIBE_CLUSTER request flows through the router's dynamic path, which
            // calls TopologyService.topicNames() before forwarding the request upstream
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(TopologyCapturingRouterFactory.capturedTopicNames.get())
                    .as("Expect admin.describeCluster() to have driven id resolution")
                    .containsEntry(topicId, topicName);
        }
    }

    @Test
    void invalidateRouteShouldForceFreshMetadataLookup(KafkaCluster cluster) throws Exception {
        // Given
        var config = topologyConfig(cluster);
        var topicName = "topology-invalidate-" + UUID.randomUUID();

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var admin = tester.admin()) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
            var topicId = toInternalUuid(admin.describeTopics(List.of(topicName)).allTopicNames()
                    .get(10, TimeUnit.SECONDS).get(topicName).topicId());
            TopologyCapturingRouterFactory.topicIdsToResolve.set(Set.of(topicId));

            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);
            assertThat(TopologyCapturingRouterFactory.capturedTopicNames.get())
                    .as("cache warmed by the first lookup")
                    .containsEntry(topicId, topicName);

            admin.deleteTopics(List.of(topicName)).all().get(10, TimeUnit.SECONDS);

            // When: querying again without invalidating - the cache hit means no fresh
            // broker round-trip happens, so the (now stale) name is still returned
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(TopologyCapturingRouterFactory.capturedTopicNames.get())
                    .as("stale cached name still served without invalidation")
                    .containsEntry(topicId, topicName);

            // When: invalidateRoute() forces the next lookup to hit the broker again, which can
            // no longer resolve the now-deleted topic
            TopologyCapturingRouterFactory.capturedTopologyService.get().invalidateRoute(ROUTE_NAME);
            admin.describeCluster().clusterId().get(10, TimeUnit.SECONDS);

            // Then
            assertThat(TopologyCapturingRouterFactory.capturedTopicNames.get())
                    .as("deleted topic can no longer be resolved once invalidated")
                    .doesNotContainKey(topicId);
        }
    }
}
