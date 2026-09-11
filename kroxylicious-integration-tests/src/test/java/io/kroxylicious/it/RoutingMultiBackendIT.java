/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.router.AlternatingRouterFactory;
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
import io.kroxylicious.testing.kafka.common.ClientConfig;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;
import io.kroxylicious.testing.kafka.junit5ext.Name;
import io.kroxylicious.testing.kafka.junit5ext.Topic;
import io.kroxylicious.testing.kafka.junit5ext.TopicNameMethodSource;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that a router can actively switch between multiple backing Kafka
 * clusters within a single client session. Uses {@link AlternatingRouterFactory} to alternate
 * PRODUCE requests in batches between two independent clusters.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingMultiBackendIT {

    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    private static final String TOPIC = "routing-multi-backend-test";
    private static final int BATCH_SIZE = 10;
    private static final int TOTAL_RECORDS = 30;

    @Name("clusterA")
    static KafkaCluster clusterA;
    @Name("clusterB")
    static KafkaCluster clusterB;

    @Name("clusterA")
    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "direct-verify-a")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    static Consumer<String, String> consumerA;

    @Name("clusterB")
    @ClientConfig(name = ConsumerConfig.GROUP_ID_CONFIG, value = "direct-verify-b")
    @ClientConfig(name = ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, value = "earliest")
    static Consumer<String, String> consumerB;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterA")
    @TopicNameMethodSource(value = "fixedTopicName")
    static Topic clusterATopic;

    @SuppressWarnings("unused") // topic creation managed by KafkaClusterExtension
    @Name("clusterB")
    @TopicNameMethodSource(value = "fixedTopicName")
    static Topic clusterBTopic;

    @SuppressWarnings("unused") // referenced by @TopicNameMethodSource
    static String fixedTopicName() {
        return TOPIC;
    }

    private ConfigurationBuilder routingConfig() {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, List.of(), new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, List.of(), new RouteTarget("cluster-b", null));

        var routerConfig = new AlternatingRouterFactory.Config("route-a", "route-b", BATCH_SIZE);
        var routerDef = new RouterDefinition("alternating",
                AlternatingRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteTarget(null, "alternating"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldAlternateProducesBetweenBackendClusters() {
        // Given
        var config = routingConfig();

        // When
        try (var tester = KroxyliciousTesters.newBuilder(config).setFeatures(ROUTING_ENABLED).createDefaultKroxyliciousTester();
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            for (int i = 0; i < TOTAL_RECORDS; i++) {
                assertThat(producer.send(new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i)))
                        .succeedsWithin(Duration.ofSeconds(10));
            }
        }

        // Then: batch 0 (records 0-9) → cluster A, batch 1 (10-19) → cluster B, batch 2 (20-29) → cluster A
        var clusterAKeys = IntStream.concat(IntStream.range(0, BATCH_SIZE), IntStream.range(2 * BATCH_SIZE, TOTAL_RECORDS))
                .mapToObj(i -> "key-" + i)
                .toArray(String[]::new);
        var clusterBKeys = IntStream.range(BATCH_SIZE, 2 * BATCH_SIZE)
                .mapToObj(i -> "key-" + i)
                .toArray(String[]::new);

        assertThat(consumeFrom(consumerA))
                .extracting(ConsumerRecord::key)
                .containsExactly(clusterAKeys);
        assertThat(consumeFrom(consumerB))
                .extracting(ConsumerRecord::key)
                .containsExactly(clusterBKeys);
    }

    private List<ConsumerRecord<String, String>> consumeFrom(Consumer<String, String> consumer) {
        consumer.subscribe(List.of(TOPIC));
        return StreamSupport.stream(consumer.poll(Duration.ofSeconds(10)).records(TOPIC).spliterator(), false).toList();
    }
}
