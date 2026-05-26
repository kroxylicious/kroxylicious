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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration test proving that a router can actively switch between
 * multiple backing Kafka clusters within a single client session.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class RoutingMultiBackendIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingMultiBackendIT.class);
    private static final String TOPIC = "routing-test";
    private static final int BATCH_SIZE = 10;
    private static final int TOTAL_RECORDS = 30;

    static KafkaCluster clusterA;
    static KafkaCluster clusterB;

    @BeforeEach
    void createTopic() throws Exception {
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

        var routeA = new RouteDefinition("route-a", 0, null, new RouteDefinition.Target("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, null, new RouteDefinition.Target("cluster-b", null));

        var routerConfig = new AlternatingRouterFactory.Config("route-a", "route-b", BATCH_SIZE);
        var routerDef = new RouterDefinition("alternating",
                AlternatingRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteDefinition.Target(null, "alternating"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    @Test
    void shouldAlternateProducesBetweenBackendClusters() throws Exception {
        var config = routingConfig();

        try (var tester = kroxyliciousTester(config);
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

        // Batch 0 (records 0-9) → cluster A, batch 1 (10-19) → cluster B, batch 2 (20-29) → cluster A
        var recordsA = consumeDirectly(clusterA);
        var recordsB = consumeDirectly(clusterB);

        assertThat(recordsA).hasSize(20);
        assertThat(recordsB).hasSize(10);

        var valuesA = recordsA.stream()
                .map(ConsumerRecord::value)
                .toList();
        var valuesB = recordsB.stream()
                .map(ConsumerRecord::value)
                .toList();

        for (int i = 0; i < 10; i++) {
            assertThat(valuesA).contains("value-" + i);
        }
        for (int i = 10; i < 20; i++) {
            assertThat(valuesB).contains("value-" + i);
        }
        for (int i = 20; i < 30; i++) {
            assertThat(valuesA).contains("value-" + i);
        }
    }

    private List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster) {
        var props = new java.util.HashMap<>(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Set.of(TOPIC));
            List<ConsumerRecord<String, String>> all = new ArrayList<>();
            int consecutiveEmpty = 0;
            long deadline = System.currentTimeMillis() + 10_000;
            while (System.currentTimeMillis() < deadline && consecutiveEmpty < 3) {
                ConsumerRecords<String, String> batch = consumer.poll(Duration.ofMillis(500));
                batch.forEach(all::add);
                if (batch.isEmpty() && !all.isEmpty()) {
                    consecutiveEmpty++;
                }
                else if (!batch.isEmpty()) {
                    consecutiveEmpty = 0;
                }
            }
            return all;
        }
    }
}
