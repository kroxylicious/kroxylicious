/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import io.kroxylicious.kafka.common.protocol.ApiKeys;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.ProduceCountingFilter;
import io.kroxylicious.it.testplugins.ProduceCountingFilterFactory;
import io.kroxylicious.it.testplugins.RequestCountingFilter;
import io.kroxylicious.it.testplugins.RequestCountingFilterFactory;
import io.kroxylicious.it.testplugins.router.ClientIdRouterFactory;
import io.kroxylicious.it.testplugins.router.DynamicProduceRouterFactory;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.internal.config.Feature;
import io.kroxylicious.proxy.internal.config.Features;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.OS_ASSIGNED_BOOTSTRAP;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for route filters interacting with nested routing.
 * Verifies that filters on the outer route (targeting a nested router) and
 * filters on the inner route (targeting a cluster) both fire correctly,
 * and that the same filter name on different routers' routes counts
 * independently with correct response routing.
 *
 * <pre>
 * Outer router (ClientIdRouterFactory):
 *   route "direct"  (id=0) → cluster-A                     (client "go-direct")
 *   route "nested"  (id=1) → inner router, filter: outer-counter
 *
 * Inner router (DynamicProduceRouterFactory):
 *   route "backend" (id=0) → cluster-B, filter: inner-counter
 * </pre>
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class NestedRouteWithFiltersIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    @Name("clusterA")
    static KafkaCluster clusterA;
    @Name("clusterB")
    static KafkaCluster clusterB;

    private ConfigurationBuilder nestedRoutingWithFilters(String outerCounterId, String innerCounterId) {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var outerCounterFilter = new NamedFilterDefinitionBuilder(
                "outer-counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", outerCounterId)
                .build();
        var innerCounterFilter = new NamedFilterDefinitionBuilder(
                "inner-counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", innerCounterId)
                .build();

        // Inner router: routes everything dynamically to "backend", with inner-counter filter
        var innerRoute = new RouteDefinition("backend", 0, List.of("inner-counter"), new RouteTarget("cluster-b", null));
        var innerConfig = new DynamicProduceRouterFactory.Config("backend");
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(), innerConfig, List.of(innerRoute));

        // Outer router: routes by client ID, "nested" route targets inner router with outer-counter filter
        var directRoute = new RouteDefinition("direct", 0, List.of(), new RouteTarget("cluster-a", null));
        var nestedRoute = new RouteDefinition("nested", 1, List.of("outer-counter"), new RouteTarget(null, "inner"));
        var outerConfig = new ClientIdRouterFactory.Config(
                Map.of("go-direct", "direct", "go-nested", "nested"),
                "nested");
        var outerRouter = new RouterDefinition("outer",
                ClientIdRouterFactory.class.getName(), outerConfig, List.of(directRoute, nestedRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToFilterDefinitions(outerCounterFilter, innerCounterFilter)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);
    }

    @Test
    void directRouteShouldBypassNestedRouter(
                                             @Name("clusterA") Topic topicOnA,
                                             @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                             @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB) {
        // Given
        String topic = topicOnA.name();
        createTopicOnCluster(clusterB, topic);
        String outerCtr = "direct-outer-" + topic;
        String innerCtr = "direct-inner-" + topic;
        var config = nestedRoutingWithFilters(outerCtr, innerCtr);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "go-direct",
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 3; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyA, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2");
        assertThat(consumeFrom(verifyB, topic)).isEmpty();
        assertThat(RequestCountingFilter.countFor(outerCtr, ApiKeys.PRODUCE))
                .as("outer filter should not fire for direct route")
                .isZero();
        assertThat(RequestCountingFilter.countFor(innerCtr, ApiKeys.PRODUCE))
                .as("inner filter should not fire for direct route")
                .isZero();
    }

    @Test
    void nestedRouteShouldFireBothOuterAndInnerFilters(
                                                       @Name("clusterB") Topic topicOnB,
                                                       @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                       @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB) {
        // Given
        String topic = topicOnB.name();
        createTopicOnCluster(clusterA, topic);
        String outerCtr = "nested-outer-" + topic;
        String innerCtr = "nested-inner-" + topic;
        var config = nestedRoutingWithFilters(outerCtr, innerCtr);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "go-nested",
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 3; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyA, topic)).isEmpty();
        assertThat(consumeFrom(verifyB, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2");
        assertThat(RequestCountingFilter.countFor(outerCtr, ApiKeys.PRODUCE))
                .as("outer filter should fire for each produce through nested route")
                .isEqualTo(3);
        assertThat(RequestCountingFilter.countFor(innerCtr, ApiKeys.PRODUCE))
                .as("inner filter should fire for each produce through nested route")
                .isEqualTo(3);
    }

    @Test
    void sameFilterNameOnDifferentRoutersShouldCountIndependently(
                                                                  @Name("clusterB") Topic topicOnB,
                                                                  @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB) {
        // Given
        String topic = topicOnB.name();
        String counterId = "shared-" + topic;

        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var sharedFilter = new NamedFilterDefinitionBuilder(
                "shared-counter", RequestCountingFilterFactory.class.getName())
                .withConfig("counterId", counterId)
                .build();

        var innerRoute = new RouteDefinition("backend", 0, List.of("shared-counter"), new RouteTarget("cluster-b", null));
        var innerConfig = new DynamicProduceRouterFactory.Config("backend");
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(), innerConfig, List.of(innerRoute));

        var nestedRoute = new RouteDefinition("nested", 0, List.of("shared-counter"), new RouteTarget(null, "inner"));
        var outerConfig = new DynamicProduceRouterFactory.Config("nested");
        var outerRouter = new RouterDefinition("outer",
                DynamicProduceRouterFactory.class.getName(), outerConfig, List.of(nestedRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(targetB)
                .addToFilterDefinitions(sharedFilter)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 3; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyB, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2");
        assertThat(RequestCountingFilter.countFor(counterId, ApiKeys.PRODUCE))
                .as("filter fires once at each nesting level: 3 requests × 2 levels = 6")
                .isEqualTo(6);
    }

    /**
     * Verifies that the DecodePredicate accounts for nested router filter decode
     * requirements. The inner route uses a ProduceRequestFilter (narrow decode scope:
     * only Produce) with no filter on the outer route. If the DecodePredicate only
     * considered top-level filters, Produce requests would arrive undecoded at the
     * inner filter and it would never fire.
     */
    @Test
    void innerRouterFilterWithSpecificDecodeRequirementsShouldWork(
                                                                   @Name("clusterB") Topic topicOnB,
                                                                   @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB) {
        // Given
        String topic = topicOnB.name();
        String innerCtr = "decode-inner-" + topic;

        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var innerProduceFilter = new NamedFilterDefinitionBuilder(
                "produce-counter", ProduceCountingFilterFactory.class.getName())
                .withConfig("counterId", innerCtr)
                .build();

        // Inner router: routes dynamically to "backend" with a ProduceRequestFilter (narrow decode)
        var innerRoute = new RouteDefinition("backend", 0, List.of("produce-counter"), new RouteTarget("cluster-b", null));
        var innerConfig = new DynamicProduceRouterFactory.Config("backend");
        var innerRouter = new RouterDefinition("inner",
                DynamicProduceRouterFactory.class.getName(), innerConfig, List.of(innerRoute));

        // Outer router: single route targeting inner router, NO filters on this route
        var nestedRoute = new RouteDefinition("to-inner", 0, List.of(), new RouteTarget(null, "inner"));
        var outerConfig = new DynamicProduceRouterFactory.Config("to-inner");
        var outerRouter = new RouterDefinition("outer",
                DynamicProduceRouterFactory.class.getName(), outerConfig, List.of(nestedRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder(OS_ASSIGNED_BOOTSTRAP).build())
                .build();

        var config = baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToFilterDefinitions(innerProduceFilter)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 3; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyB, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2");
        assertThat(ProduceCountingFilter.countFor(innerCtr))
                .as("inner ProduceRequestFilter should fire for each produce through nested route")
                .isEqualTo(3);
    }

    private static void createTopicOnCluster(KafkaCluster cluster, String topicName) {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()))) {
            assertThat(admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all())
                    .succeedsWithin(Duration.ofSeconds(10));
        }
    }

    private List<ConsumerRecord<String, String>> consumeFrom(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(List.of(topic));
        return StreamSupport.stream(consumer.poll(Duration.ofSeconds(10)).records(topic).spliterator(), false).toList();
    }
}
