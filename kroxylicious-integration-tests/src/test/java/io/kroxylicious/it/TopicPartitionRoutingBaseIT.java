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
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;
import io.micrometer.core.instrument.Metrics;

import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.router.topic.TopicPartitionRouterFactory;
import io.kroxylicious.proxy.router.topic.config.RouteConfig;
import io.kroxylicious.proxy.router.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;
import io.kroxylicious.testing.integration.tester.KroxyliciousTester;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared infrastructure for topic-partition router integration tests.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
abstract class TopicPartitionRoutingBaseIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRoutingBaseIT.class);

    @BrokerCluster(numBrokers = 3)
    static KafkaCluster clusterA;
    @BrokerCluster(numBrokers = 3)
    static KafkaCluster clusterB;

    RoutingEventCaptor routingCaptor;

    @BeforeEach
    void setUp() throws Exception {
        routingCaptor = RoutingEventCaptor.install();
        LOGGER.info("clusterA bootstrap: {}, clusterB bootstrap: {}",
                clusterA.getBootstrapServers(), clusterB.getBootstrapServers());
        assertThat(clusterA.getBootstrapServers())
                .as("clusters must be distinct instances")
                .isNotEqualTo(clusterB.getBootstrapServers());
    }

    @AfterEach
    void tearDown() {
        if (routingCaptor != null) {
            routingCaptor.close();
        }
        assertThat(Metrics.globalRegistry.getMeters())
                .as("fetch session metrics should be deregistered after proxy shutdown")
                .filteredOn(m -> m.getId().getName().startsWith("kroxylicious_fetch_session_"))
                .isEmpty();
    }

    static void createTopicOnCluster(String topicName,
                                     int partitions,
                                     KafkaCluster cluster)
            throws Exception {
        var newTopic = new NewTopic(topicName, Optional.of(partitions), Optional.empty());
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
    }

    void createTopic(String topicName, KafkaCluster... clusters) throws Exception {
        for (var cluster : clusters) {
            createTopicOnCluster(topicName, 1, cluster);
        }
    }

    ConfigurationBuilder topicRouterConfig() {
        return topicRouterConfig(clusterA, clusterB);
    }

    ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                           KafkaCluster b) {
        return topicRouterConfig(a, b, null);
    }

    ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                           KafkaCluster b,
                                           int maxFetchSessionCacheSlots,
                                           Duration minFetchSessionEviction) {
        return topicRouterConfig(a, b, null, maxFetchSessionCacheSlots, minFetchSessionEviction);
    }

    ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                           KafkaCluster b,
                                           Duration producerIdTtl) {
        return topicRouterConfig(a, b, producerIdTtl, null, null);
    }

    ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                           KafkaCluster b,
                                           Duration producerIdTtl,
                                           Integer maxFetchSessionCacheSlots,
                                           Duration minFetchSessionEviction) {
        var targetA = new TargetClusterDefinition("cluster-a", a.getBootstrapServers(), null);
        var targetB = new TargetClusterDefinition("cluster-b", b.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", null, new RouteDefinition.Target("cluster-a", null));
        var routeB = new RouteDefinition("route-b", null, new RouteDefinition.Target("cluster-b", null));

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(
                        new RouteConfig("route-a", List.of("a.")),
                        new RouteConfig("route-b", List.of("b."))),
                producerIdTtl,
                maxFetchSessionCacheSlots,
                minFetchSessionEviction);

        var routerDef = new RouterDefinition("topic-router",
                TopicPartitionRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withTarget(new RouteDefinition.Target(null, "topic-router"))
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192")
                        .editPortIdentifiesNode()
                        .addToNodeIdRanges(new NamedRange("brokers", 0, 5))
                        .endPortIdentifiesNode()
                        .build())
                .build();

        return baseConfigurationBuilder()
                .addToClusterDefinitions(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToVirtualClusters(vc);
    }

    static void negotiateApiVersions(KafkaClient client) {
        client.getSync(new Request(
                API_VERSIONS,
                API_VERSIONS.latestVersion(),
                "test-client",
                new ApiVersionsRequestData()
                        .setClientSoftwareName("test")
                        .setClientSoftwareVersion("1.0")));
    }

    /**
     * Fetches metadata for the given topics via the proxy, returning the full response
     * including broker addresses (rewritten by BrokerAddressFilter to proxy addresses).
     */
    static MetadataResponseData fetchMetadata(KafkaClient client,
                                              String... topicNames) {
        var request = new MetadataRequestData();
        for (var name : topicNames) {
            request.topics().add(new MetadataRequestData.MetadataRequestTopic().setName(name));
        }
        var response = client.getSync(new Request(
                ApiKeys.METADATA, ApiKeys.METADATA.latestVersion(), "test-client", request));
        return (MetadataResponseData) response.payload().message();
    }

    /**
     * Returns the proxy address (host:port) of the leader for the given topic partition,
     * as advertised in a metadata response from the proxy.
     */
    static String leaderAddress(MetadataResponseData metadata,
                                String topicName,
                                int partitionIndex) {
        for (var topic : metadata.topics()) {
            if (topic.name().equals(topicName)) {
                for (var partition : topic.partitions()) {
                    if (partition.partitionIndex() == partitionIndex) {
                        int leaderId = partition.leaderId();
                        for (var broker : metadata.brokers()) {
                            if (broker.nodeId() == leaderId) {
                                return broker.host() + ":" + broker.port();
                            }
                        }
                        throw new IllegalStateException(
                                "Leader " + leaderId + " for " + topicName + "-" + partitionIndex
                                        + " not found in broker list");
                    }
                }
                throw new IllegalStateException(
                        "Partition " + partitionIndex + " not found for topic " + topicName);
            }
        }
        throw new IllegalStateException("Topic " + topicName + " not found in metadata response");
    }

    /**
     * Opens a simple test client connected to the leader for the given topic partition.
     * Performs API version negotiation before returning.
     */
    static KafkaClient clientForLeader(KroxyliciousTester tester,
                                       MetadataResponseData metadata,
                                       String topicName,
                                       int partitionIndex) {
        String address = leaderAddress(metadata, topicName, partitionIndex);
        var client = tester.simpleTestClient(address, false);
        negotiateApiVersions(client);
        return client;
    }

    List<ConsumerRecord<String, String>> consumeDirectly(KafkaCluster cluster,
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

    static void warmUpGroupCoordinator(KafkaCluster cluster,
                                       String topic,
                                       String groupId) {
        var props = new java.util.HashMap<>(cluster.getKafkaClientConfiguration());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        try (var consumer = new KafkaConsumer<String, String>(props)) {
            consumer.assign(List.of(new TopicPartition(topic, 0)));
            consumer.commitSync(Map.of(
                    new TopicPartition(topic, 0), new OffsetAndMetadata(0)));
        }
    }
}
