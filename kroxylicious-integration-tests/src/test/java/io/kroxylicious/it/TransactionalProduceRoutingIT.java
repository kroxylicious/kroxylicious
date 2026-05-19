/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.TargetClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.routing.topic.TopicPartitionRouterFactory;
import io.kroxylicious.proxy.routing.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.proxy.routing.topic.config.TopicRoute;
import io.kroxylicious.testing.integration.config.NamedFilterDefinitionBuilder;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.common.SaslMechanism;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for transactional produce routing through the topic-partition router
 * using subject-based transaction route mapping.
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class TransactionalProduceRoutingIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalProduceRoutingIT.class);

    private RoutingEventCaptor routingCaptor;

    @BeforeEach
    void setUp() {
        routingCaptor = RoutingEventCaptor.install();
    }

    @AfterEach
    void tearDown() {
        if (routingCaptor != null) {
            routingCaptor.close();
        }
    }

    /**
     * User "alice" has no entry in {@code transactionalUserRoutes}, so her
     * transaction coordinator is discovered on the default route (route-a / clusterA).
     * A committed record on an {@code a.*} topic should be visible at {@code read_committed}.
     */
    @Test
    void shouldCommitTransactionOnDefaultRoute(
                                               @SaslMechanism(value = "PLAIN", principals = {
                                                       @SaslMechanism.Principal(user = "alice", password = "alice-secret"),
                                                       @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                               }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                               @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "a.txn-default";
        createTopicOnCluster(topic, 1, clusterA);

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            Map<String, Object> producerConfig = saslProducerConfig(
                    tester.getBootstrapAddress(), "alice", "alice-secret", "txn-alice-1");
            try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "key1", "val1")).get(10, TimeUnit.SECONDS);
                producer.commitTransaction();
            }
        }

        var records = consumeDirectly(clusterA, topic, "read_committed");
        assertThat(records).hasSize(1);
        assertThat(records.iterator().next().value()).isEqualTo("val1");
    }

    /**
     * User "bob" is mapped to route-b in {@code transactionalUserRoutes}, so his
     * transaction coordinator is discovered on clusterB. The producer writes to a
     * {@code b.*} topic and commits; the record should appear on clusterB at
     * {@code read_committed}. This exercises the single-route coordinator discovery
     * path in {@code discoverCoordinatorAndInitProducerId}.
     */
    @Test
    void shouldCommitTransactionOnMappedRoute(
                                              @SaslMechanism(value = "PLAIN", principals = {
                                                      @SaslMechanism.Principal(user = "alice", password = "alice-secret"),
                                                      @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                              }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                              @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "b.txn-mapped";
        createTopicOnCluster(topic, 1, clusterB);

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            Map<String, Object> producerConfig = saslProducerConfig(
                    tester.getBootstrapAddress(), "bob", "bob-secret", "txn-bob-1");
            try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "key1", "val1")).get(10, TimeUnit.SECONDS);
                producer.commitTransaction();
            }
        }

        var records = consumeDirectly(clusterB, topic, "read_committed");
        assertThat(records).hasSize(1);
        assertThat(records.iterator().next().value()).isEqualTo("val1");
    }

    /**
     * Verifies that an aborted transaction leaves no visible records when consuming
     * at {@code read_committed}. Uses the default route (no user mapping configured).
     */
    @Test
    void shouldAbortTransaction(
                                @SaslMechanism(value = "PLAIN", principals = {
                                        @SaslMechanism.Principal(user = "alice", password = "alice-secret")
                                }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "a.txn-abort";
        createTopicOnCluster(topic, 1, clusterA);

        var config = buildConfig(clusterA, clusterB, Map.of());

        try (var tester = kroxyliciousTester(config)) {
            Map<String, Object> producerConfig = saslProducerConfig(
                    tester.getBootstrapAddress(), "alice", "alice-secret", "txn-alice-abort");
            try (var producer = new KafkaProducer<String, String>(producerConfig)) {
                producer.initTransactions();
                producer.beginTransaction();
                producer.send(new ProducerRecord<>(topic, "key1", "val1")).get(10, TimeUnit.SECONDS);
                producer.abortTransaction();
            }
        }

        var records = consumeDirectly(clusterA, topic, "read_committed");
        assertThat(records).isEmpty();
    }

    // --- config helpers ---

    private ConfigurationBuilder buildConfig(KafkaCluster clusterA,
                                             KafkaCluster clusterB,
                                             Map<String, String> transactionalUserRoutes) {
        var targetA = new TargetClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new TargetClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", null, "cluster-a", null);
        var routeB = new RouteDefinition("route-b", null, "cluster-b", null);

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(
                        new TopicRoute("route-a", List.of("a.")),
                        new TopicRoute("route-b", List.of("b."))),
                transactionalUserRoutes.isEmpty() ? null : transactionalUserRoutes,
                null, null, null);

        var routerDef = new RouterDefinition("topic-router",
                TopicPartitionRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var saslFilter = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName())
                .withConfig("enabledMechanisms", Set.of("PLAIN"))
                .build();

        var vc = new VirtualClusterBuilder()
                .withName("demo")
                .withRouter("topic-router")
                .addToGateways(defaultPortIdentifiesNodeGatewayBuilder("localhost:9192")
                        .editPortIdentifiesNode()
                        .addToNodeIdRanges(new NamedRange("brokers", 0, 5))
                        .endPortIdentifiesNode()
                        .build())
                .build();

        return baseConfigurationBuilder()
                .addToTargetClusters(targetA, targetB)
                .addToRouterDefinitions(routerDef)
                .addToFilterDefinitions(saslFilter)
                .addToDefaultFilters(saslFilter.name())
                .addToVirtualClusters(vc);
    }

    private static Map<String, Object> saslProducerConfig(String bootstrap,
                                                          String username,
                                                          String password,
                                                          String transactionalId) {
        var config = new HashMap<String, Object>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                        + "  username=\"" + username + "\"\n"
                        + "  password=\"" + password + "\";");
        return config;
    }

    // --- test infrastructure ---

    private static void createTopicOnCluster(String topicName,
                                             int partitions,
                                             KafkaCluster cluster)
            throws Exception {
        var newTopic = new NewTopic(topicName, Optional.of(partitions), Optional.empty());
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
    }

    private static Iterable<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>> consumeDirectly(
                                                                                                              KafkaCluster cluster,
                                                                                                              String topic,
                                                                                                              String isolationLevel) {
        var config = new HashMap<>(cluster.getKafkaClientConfiguration());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        var records = new java.util.ArrayList<org.apache.kafka.clients.consumer.ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> polled = consumer.poll(Duration.ofMillis(500));
                polled.forEach(records::add);
                if (!records.isEmpty()) {
                    break;
                }
            }
        }
        return records;
    }
}
