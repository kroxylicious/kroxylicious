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
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.AlternatingRouterFactory;
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
 * Integration test proving that a router can actively switch between multiple backing Kafka
 * clusters within a single client session. Uses {@link AlternatingRouterFactory} to alternate
 * PRODUCE requests in batches between two independent clusters.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingMultiBackendIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingMultiBackendIT.class);
    private static final Features ROUTING_ENABLED = Features.builder().enable(Feature.ROUTING).build();

    private static final String TOPIC = "routing-multi-backend-test";
    private static final int BATCH_SIZE = 10;
    private static final int TOTAL_RECORDS = 30;

    static KafkaCluster clusterA;
    static KafkaCluster clusterB;

    @BeforeEach
    void createTopics() throws Exception {
        LOGGER.info("clusterA bootstrap: {}, clusterB bootstrap: {}",
                clusterA.getBootstrapServers(), clusterB.getBootstrapServers());
        assertThat(clusterA.getBootstrapServers())
                .as("clusters must be distinct instances")
                .isNotEqualTo(clusterB.getBootstrapServers());

        var newTopic = new NewTopic(TOPIC, Optional.of(1), Optional.empty());
        try (var admin = AdminClient.create(clusterA.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
        try (var admin = AdminClient.create(clusterB.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
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
    void shouldAlternateProducesBetweenBackendClusters() throws Exception {
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
                producer.send(new ProducerRecord<>(TOPIC, "key-" + i, "value-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        // Then: batch 0 (records 0-9) → cluster A, batch 1 (10-19) → cluster B, batch 2 (20-29) → cluster A
        var recordsA = consumeDirectly(clusterA);
        var recordsB = consumeDirectly(clusterB);

        LOGGER.info("Records on cluster A: {}, cluster B: {}", recordsA.size(), recordsB.size());
        assertThat(recordsA).as("cluster A should have two batches (0 and 2)").hasSize(2 * BATCH_SIZE);
        assertThat(recordsB).as("cluster B should have one batch (1)").hasSize(BATCH_SIZE);
    }

    private List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster) throws Exception {
        var props = new java.util.Properties();
        props.putAll(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "direct-verify-" + cluster.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (var consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline) {
                var records = consumer.poll(Duration.ofMillis(500));
                records.forEach(result::add);
                if (!records.isEmpty()) {
                    break;
                }
            }
        }
        return result;
    }
}
