/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.net.URI;
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
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.routing.topic.TopicPartitionRouterFactory;
import io.kroxylicious.proxy.routing.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.proxy.routing.topic.config.TopicRoute;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.Response;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link TopicPartitionRouterFactory}.
 * Covers version capping (Phase 1) and produce fan-out (Phase 2).
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class TopicPartitionRouterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouterIT.class);

    static KafkaCluster clusterA;
    static KafkaCluster clusterB;

    @BeforeEach
    void setUp() throws Exception {
        LOGGER.info("clusterA bootstrap: {}, clusterB bootstrap: {}",
                clusterA.getBootstrapServers(), clusterB.getBootstrapServers());
        assertThat(clusterA.getBootstrapServers())
                .as("clusters must be distinct instances")
                .isNotEqualTo(clusterB.getBootstrapServers());
    }

    private void createTopic(String topicName, KafkaCluster... clusters) throws Exception {
        var newTopic = new NewTopic(topicName, Optional.of(1), Optional.empty());
        for (var cluster : clusters) {
            try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
                admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private ConfigurationBuilder topicRouterConfig() {
        var targetA = new TargetClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new TargetClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", null, "cluster-a", null);
        var routeB = new RouteDefinition("route-b", null, "cluster-b", null);

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(
                        new TopicRoute("route-a", List.of("a.")),
                        new TopicRoute("route-b", List.of("b."))));

        var routerDef = new RouterDefinition("topic-router",
                TopicPartitionRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withRouter("topic-router")
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192").build())
                .build();

        return baseConfigurationBuilder()
                .addToTargetClusters(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    // --- Phase 1: version capping ---

    @Test
    void shouldCapApiVersionsForTopicIdBearingKeys() throws Exception {
        createTopic("a.dummy", clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            var address = URI.create("kafka://" + tester.getBootstrapAddress());
            try (var client = new KafkaClient(address.getHost(), address.getPort())) {
                var request = new Request(
                        ApiKeys.API_VERSIONS,
                        ApiKeys.API_VERSIONS.latestVersion(),
                        "test-client",
                        new ApiVersionsRequestData()
                                .setClientSoftwareName("test")
                                .setClientSoftwareVersion("1.0"));

                Response response = client.getSync(request);
                var body = (ApiVersionsResponseData) response.payload().message();

                var produceVersion = body.apiKeys().find(ApiKeys.PRODUCE.id);
                assertThat(produceVersion).isNotNull();
                assertThat(produceVersion.maxVersion()).isLessThanOrEqualTo((short) 12);

                var fetchVersion = body.apiKeys().find(ApiKeys.FETCH.id);
                assertThat(fetchVersion).isNotNull();
                assertThat(fetchVersion.maxVersion()).isLessThanOrEqualTo((short) 12);

                var offsetCommitVersion = body.apiKeys().find(ApiKeys.OFFSET_COMMIT.id);
                assertThat(offsetCommitVersion).isNotNull();
                assertThat(offsetCommitVersion.maxVersion()).isLessThanOrEqualTo((short) 9);

                var offsetFetchVersion = body.apiKeys().find(ApiKeys.OFFSET_FETCH.id);
                assertThat(offsetFetchVersion).isNotNull();
                assertThat(offsetFetchVersion.maxVersion()).isLessThanOrEqualTo((short) 9);

                var deleteTopicsVersion = body.apiKeys().find(ApiKeys.DELETE_TOPICS.id);
                assertThat(deleteTopicsVersion).isNotNull();
                assertThat(deleteTopicsVersion.maxVersion()).isLessThanOrEqualTo((short) 5);
            }
        }
    }

    @Test
    void shouldPassThroughProduceAndConsumeWithSingleRoute() throws Exception {
        String topic = "a.passthrough";
        createTopic(topic, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topic, "key-" + i, "val-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        var records = consumeDirectly(clusterA, topic);
        assertThat(records).hasSize(5);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("val-0", "val-1", "val-2", "val-3", "val-4");
    }

    // --- Phase 2: produce fan-out ---

    @Test
    void shouldFanOutProduceToMultipleClusters() throws Exception {
        String topicA = "a.orders";
        String topicB = "b.logs";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topicA, "key-a1", "val-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "key-b1", "val-b1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicA, "key-a2", "val-a2"))
                    .get(10, TimeUnit.SECONDS);
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(2);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactlyInAnyOrder("val-a1", "val-a2");

        assertThat(recordsB).hasSize(1);
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("val-b1");
    }

    @Test
    void shouldFanOutAcksZeroProduce() throws Exception {
        String topicA = "a.fire";
        String topicB = "b.forget";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0,
                        "acks", "0"))) {
            producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"));
            producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"));
            producer.flush();
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(1);
        assertThat(recordsB).hasSize(1);
    }

    @Test
    void shouldRouteUnprefixedTopicToDefaultRoute() throws Exception {
        String topic = "unprefixed-topic";
        createTopic(topic, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topic, "key", "val"))
                    .get(10, TimeUnit.SECONDS);
        }

        var records = consumeDirectly(clusterA, topic);
        assertThat(records).hasSize(1);
        assertThat(records.get(0).value()).isEqualTo("val");
    }

    private List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster,
                                                                 String topic) {
        var props = new java.util.HashMap<>(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + System.nanoTime());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.subscribe(Set.of(topic));
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
