/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

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
import io.kroxylicious.testing.integration.tester.KroxyliciousTesters;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.api.TerminationStyle;
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that nested routing works: an outer router
 * forwards requests to an inner router, which then forwards to the
 * correct backing cluster.
 *
 * <p>The outer router ({@link DynamicProduceRouterFactory}) dynamically
 * routes all PRODUCE requests to a single route targeting the inner
 * router. The inner router ({@link ClientIdRouterFactory}) routes based
 * on the Kafka client ID to one of two backend clusters.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class NestedRoutingIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    @Name("clusterA")
    static KafkaCluster clusterA;
    @Name("clusterB")
    static KafkaCluster clusterB;

    private ConfigurationBuilder nestedRoutingConfig() {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        // Inner router: routes by client ID to one of two clusters
        var innerRouteA = new RouteDefinition("backend-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var innerRouteB = new RouteDefinition("backend-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var innerConfig = new ClientIdRouterFactory.Config(
                Map.of("app-a", "backend-a", "app-b", "backend-b"),
                "backend-a");
        var innerRouter = new RouterDefinition("inner",
                ClientIdRouterFactory.class.getName(), innerConfig, List.of(innerRouteA, innerRouteB));

        // Outer router: routes all PRODUCE dynamically to the inner router
        var outerRoute = new RouteDefinition("to-inner", 0, List.of(), new RouteTarget(null, "inner"));
        var outerConfig = new DynamicProduceRouterFactory.Config("to-inner");
        var outerRouter = new RouterDefinition("outer",
                DynamicProduceRouterFactory.class.getName(), outerConfig, List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(outerRouter, innerRouter)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldRouteProduceThroughNestedRouterToClusterA(
                                                         @Name("clusterA") Topic topicOnA,
                                                         @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                         @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        String topic = topicOnA.name();
        createTopicOnCluster(clusterB, topic);
        var config = nestedRoutingConfig();

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "app-a",
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 5; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyA, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2", "key-3", "key-4");
        assertThat(consumeFrom(verifyB, topic)).isEmpty();
    }

    @Test
    void shouldRouteProduceThroughNestedRouterToClusterB(
                                                         @Name("clusterB") Topic topicOnB,
                                                         @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                         @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        String topic = topicOnB.name();
        createTopicOnCluster(clusterA, topic);
        var config = nestedRoutingConfig();

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "app-b",
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
    }

    // --- 3-level nesting: outer → middle → inner → clusters ---

    private ConfigurationBuilder deeplyNestedRoutingConfig() {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        // Inner router: routes by client ID to one of two clusters
        var innerRouteA = new RouteDefinition("backend-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var innerRouteB = new RouteDefinition("backend-b", 1, List.of(), new RouteTarget("cluster-b", null));
        var innerConfig = new ClientIdRouterFactory.Config(
                Map.of("app-a", "backend-a", "app-b", "backend-b"),
                "backend-a");
        var innerRouter = new RouterDefinition("inner",
                ClientIdRouterFactory.class.getName(), innerConfig, List.of(innerRouteA, innerRouteB));

        // Middle router: forwards everything to the inner router
        var middleRoute = new RouteDefinition("to-inner", 0, List.of(), new RouteTarget(null, "inner"));
        var middleConfig = new DynamicProduceRouterFactory.Config("to-inner");
        var middleRouter = new RouterDefinition("middle",
                DynamicProduceRouterFactory.class.getName(), middleConfig, List.of(middleRoute));

        // Outer router: forwards everything to the middle router
        var outerRoute = new RouteDefinition("to-middle", 0, List.of(), new RouteTarget(null, "middle"));
        var outerConfig = new DynamicProduceRouterFactory.Config("to-middle");
        var outerRouter = new RouterDefinition("outer",
                DynamicProduceRouterFactory.class.getName(), outerConfig, List.of(outerRoute));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "outer"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(outerRouter, middleRouter, innerRouter)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldRouteProduceThroughDeeplyNestedRoutersToClusterA(
                                                                @Name("clusterA") Topic topicOnA,
                                                                @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                                @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        String topic = topicOnA.name();
        createTopicOnCluster(clusterB, topic);
        var config = deeplyNestedRoutingConfig();

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "app-a",
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < 5; i++) {
                assertThat(producer.send(new ProducerRecord<>(topic, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then
        assertThat(consumeFrom(verifyA, topic))
                .extracting(ConsumerRecord::key)
                .containsExactly("key-0", "key-1", "key-2", "key-3", "key-4");
        assertThat(consumeFrom(verifyB, topic)).isEmpty();
    }

    @Test
    void shouldRouteProduceThroughDeeplyNestedRoutersToClusterB(
                                                                @Name("clusterB") Topic topicOnB,
                                                                @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                                @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB)
            throws ExecutionException, InterruptedException, TimeoutException {
        // Given
        String topic = topicOnB.name();
        createTopicOnCluster(clusterA, topic);
        var config = deeplyNestedRoutingConfig();

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "app-b",
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
    }

    // --- error propagation ---

    @Test
    void shouldPropagateErrorWhenBackingClusterIsUnreachable(
                                                             @Name("clusterA") Topic topicOnA)
            throws Exception {
        // Given
        String topic = topicOnA.name();
        var config = nestedRoutingConfig();

        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "client.id", "app-a",
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0,
                        "delivery.timeout.ms", 5000,
                        "request.timeout.ms", 3000))) {

            assertThat(producer.send(new ProducerRecord<>(topic, "baseline", "ok")))
                    .succeedsWithin(Duration.ofSeconds(10));

            // When
            clusterA.stopNodes(u -> true, TerminationStyle.GRACEFUL);
            try {
                // Then
                assertThat(producer.send(new ProducerRecord<>(topic, "should-fail", "fail")))
                        .failsWithin(Duration.ofSeconds(10));
            }
            finally {
                clusterA.startNodes(u -> true);
            }
        }
    }

    // --- concurrency ---

    @Test
    void shouldHandleConcurrentProducersRoutingToDifferentClusters(
                                                                   @Name("clusterA") Topic topicOnA,
                                                                   @Name("clusterA") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-a") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyA,
                                                                   @Name("clusterB") @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "verify-b") @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest") Consumer<String, String> verifyB)
            throws Exception {
        // Given
        String topic = topicOnA.name();
        createTopicOnCluster(clusterB, topic);
        var config = nestedRoutingConfig();
        int producersPerCluster = 2;
        int messagesPerProducer = 3;

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester()) {
            ExecutorService executor = Executors.newFixedThreadPool(producersPerCluster * 2);
            try {
                List<Future<?>> futures = new ArrayList<>();
                for (int p = 0; p < producersPerCluster; p++) {
                    int idx = p;
                    futures.add(executor.submit(() -> {
                        try (var producer = tester.producer(Map.of(
                                "client.id", "app-a",
                                "enable.idempotence", false,
                                "retries", 0,
                                "batch.size", 0,
                                "linger.ms", 0))) {
                            for (int i = 0; i < messagesPerProducer; i++) {
                                producer.send(new ProducerRecord<>(topic, "a-" + idx + "-" + i, "v")).get(30, TimeUnit.SECONDS);
                            }
                        }
                        return null;
                    }));
                    futures.add(executor.submit(() -> {
                        try (var producer = tester.producer(Map.of(
                                "client.id", "app-b",
                                "enable.idempotence", false,
                                "retries", 0,
                                "batch.size", 0,
                                "linger.ms", 0))) {
                            for (int i = 0; i < messagesPerProducer; i++) {
                                producer.send(new ProducerRecord<>(topic, "b-" + idx + "-" + i, "v")).get(30, TimeUnit.SECONDS);
                            }
                        }
                        return null;
                    }));
                }
                for (var f : futures) {
                    f.get(60, TimeUnit.SECONDS);
                }
            }
            finally {
                executor.shutdownNow();
            }
        }

        // Then
        var recordsA = consumeFrom(verifyA, topic);
        var recordsB = consumeFrom(verifyB, topic);
        assertThat(recordsA).hasSize(producersPerCluster * messagesPerProducer);
        assertThat(recordsA).extracting(ConsumerRecord::key).allSatisfy(k -> assertThat(k).startsWith("a-"));
        assertThat(recordsB).hasSize(producersPerCluster * messagesPerProducer);
        assertThat(recordsB).extracting(ConsumerRecord::key).allSatisfy(k -> assertThat(k).startsWith("b-"));
    }

    private static void createTopicOnCluster(KafkaCluster cluster, String topicName) throws ExecutionException, InterruptedException, TimeoutException {
        try (var admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(topicName, 1, (short) 1))).all().get(10, TimeUnit.SECONDS);
        }
    }

    private List<ConsumerRecord<String, String>> consumeFrom(Consumer<String, String> consumer, String topic) {
        consumer.subscribe(List.of(topic));
        return StreamSupport.stream(consumer.poll(Duration.ofSeconds(10)).records(topic).spliterator(), false).toList();
    }
}
