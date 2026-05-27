/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.ToIntFunction;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerGroupMetadata;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.message.AddOffsetsToTxnRequestData;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.router.topic.TopicPartitionRouterFactory;
import io.kroxylicious.router.topic.config.RouteConfig;
import io.kroxylicious.router.topic.config.TopicPartitionRouterConfig;
import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.integration.client.KafkaClient;
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
 * Integration tests for transactional produce router through the topic-partition router
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

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.FIND_COORDINATOR))
                .as("FIND_COORDINATOR should route to route-a for unmapped alice")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("PRODUCE should route to route-a for a.* topic")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("no produces should go to route-b")
                .isEmpty();
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

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.FIND_COORDINATOR))
                .as("FIND_COORDINATOR should route to route-b for mapped bob")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("PRODUCE should route to route-b for b.* topic")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("no produces should go to route-a")
                .isEmpty();
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

    /**
     * Exercises the exactly-once consume-transform-produce pattern: "bob" consumes
     * from a {@code b.*} topic, then commits those offsets within the transaction
     * via {@code sendOffsetsToTransaction}. This tests {@code ADD_OFFSETS_TO_TXN}
     * (routed to the transaction coordinator on route-b) and {@code TXN_OFFSET_COMMIT}
     * (routed to the group coordinator, also on route-b).
     */
    @Test
    void shouldCommitOffsetsToTransactionOnMappedRoute(
                                                       @SaslMechanism(value = "PLAIN", principals = {
                                                               @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                                       }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                                       @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String inputTopic = "b.txn-ctp-in";
        String outputTopic = "b.txn-ctp-out";
        String groupId = "cg-txn-ctp";
        createTopicOnCluster(inputTopic, 1, clusterB);
        createTopicOnCluster(outputTopic, 1, clusterB);

        var directProducerConfig = new HashMap<>(clusterB.getKafkaClientConfiguration());
        directProducerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        directProducerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (var directProducer = new KafkaProducer<String, String>(directProducerConfig)) {
            directProducer.send(new ProducerRecord<>(inputTopic, "k1", "input-val")).get(10, TimeUnit.SECONDS);
        }

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            var consumerConfig = new HashMap<String, Object>();
            consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tester.getBootstrapAddress());
            consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
            consumerConfig.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
            consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerConfig.put("security.protocol", "SASL_PLAINTEXT");
            consumerConfig.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
            consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG,
                    "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                            + "  username=\"bob\"\n  password=\"bob-secret\";");

            Map<String, Object> producerConfig = saslProducerConfig(
                    tester.getBootstrapAddress(), "bob", "bob-secret", "txn-bob-ctp");

            try (var consumer = new KafkaConsumer<String, String>(consumerConfig);
                    var producer = new KafkaProducer<String, String>(producerConfig)) {
                consumer.subscribe(List.of(inputTopic));
                producer.initTransactions();

                var polled = new AtomicReference<>(ConsumerRecords.<String, String> empty());
                Awaitility.await()
                        .atMost(Duration.ofSeconds(60))
                        .pollInterval(Duration.ofSeconds(1))
                        .untilAsserted(() -> {
                            var p = consumer.poll(Duration.ofMillis(1000));
                            if (!p.isEmpty()) {
                                polled.set(p);
                            }
                            assertThat(polled.get()).isNotEmpty();
                        });

                producer.beginTransaction();
                for (var record : polled.get()) {
                    producer.send(new ProducerRecord<>(outputTopic, record.key(),
                            "transformed-" + record.value())).get(10, TimeUnit.SECONDS);
                }
                producer.sendOffsetsToTransaction(
                        offsetsFor(polled.get(), inputTopic),
                        new ConsumerGroupMetadata(groupId));
                producer.commitTransaction();
            }
        }

        var outputRecords = consumeDirectly(clusterB, outputTopic, "read_committed");
        assertThat(outputRecords).hasSize(1);
        assertThat(outputRecords.iterator().next().value()).isEqualTo("transformed-input-val");

        try (var admin = AdminClient.create(clusterB.getKafkaClientConfiguration())) {
            var committedOffsets = admin.listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            assertThat(committedOffsets)
                    .as("transactional offset commit should be stored on clusterB")
                    .containsKey(new TopicPartition(inputTopic, 0));
            assertThat(committedOffsets.get(new TopicPartition(inputTopic, 0)).offset())
                    .as("committed offset should be 1 (after consuming 1 record)")
                    .isEqualTo(1);
        }

        var txnOffsetEvents = routingCaptor.requestsToRoute("route-b", ApiKeys.TXN_OFFSET_COMMIT);
        var addOffsetsEvents = routingCaptor.requestsToRoute("route-b", ApiKeys.ADD_OFFSETS_TO_TXN);
        assertThat(txnOffsetEvents.size() + addOffsetsEvents.size())
                .as("TXN_OFFSET_COMMIT or ADD_OFFSETS_TO_TXN should route to route-b "
                        + "(KIP-890 V2 sends only TXN_OFFSET_COMMIT v5)")
                .isGreaterThanOrEqualTo(1);
    }

    private static Map<TopicPartition, OffsetAndMetadata> offsetsFor(
                                                                     ConsumerRecords<String, String> records,
                                                                     String topic) {
        var offsets = new HashMap<TopicPartition, OffsetAndMetadata>();
        for (var record : records) {
            offsets.put(
                    new TopicPartition(topic, record.partition()),
                    new OffsetAndMetadata(record.offset() + 1));
        }
        return offsets;
    }

    // --- version sweep ---

    static List<Arguments> findCoordinatorVersions() {
        // FIND_COORDINATOR is capped at v3 by the router (v4+ uses coordinatorKeys array)
        short maxVersion = (short) Math.min(ApiKeys.FIND_COORDINATOR.latestVersion(), 3);
        var result = new ArrayList<Arguments>();
        for (short v = 1; v <= maxVersion; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    /**
     * Sweeps FIND_COORDINATOR versions (up to the capped maximum) for
     * keyType=1 (transaction coordinator). Authenticates as "bob"
     * (mapped to route-b) and verifies router to route-b.
     */
    @ParameterizedTest
    @MethodSource("findCoordinatorVersions")
    void findCoordinatorForTransactionAcrossVersions(
                                                     short apiVersion,
                                                     @SaslMechanism(value = "PLAIN", principals = {
                                                             @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                                     }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                                     @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {

            negotiateApiVersions(client);
            authenticate(client, "bob", "bob-secret");

            var findCoordReq = new FindCoordinatorRequestData()
                    .setKey("txn-sweep-v" + apiVersion)
                    .setKeyType((byte) 1);

            var body = awaitSuccessfulResponse(client,
                    ApiKeys.FIND_COORDINATOR, apiVersion, findCoordReq,
                    FindCoordinatorResponseData.class,
                    FindCoordinatorResponseData::errorCode);

            assertThat(body.nodeId())
                    .as("v%d should return a valid coordinator node", apiVersion)
                    .isGreaterThanOrEqualTo(0);
        }

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.FIND_COORDINATOR))
                .as("v%d FIND_COORDINATOR should route to route-b for bob", apiVersion)
                .isNotEmpty();
    }

    static List<Arguments> addOffsetsToTxnVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 0; v <= ApiKeys.ADD_OFFSETS_TO_TXN.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    /**
     * Sweeps ADD_OFFSETS_TO_TXN versions. Without an active transaction the
     * router returns COORDINATOR_NOT_AVAILABLE, but we verify it's handled
     * at each version without protocol errors.
     */
    @ParameterizedTest
    @MethodSource("addOffsetsToTxnVersions")
    void addOffsetsToTxnAcrossVersions(
                                       short apiVersion,
                                       @SaslMechanism(value = "PLAIN", principals = {
                                               @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                       }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                       @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {

            negotiateApiVersions(client);
            authenticate(client, "bob", "bob-secret");

            var request = new AddOffsetsToTxnRequestData()
                    .setTransactionalId("txn-sweep-offsets-v" + apiVersion)
                    .setGroupId("cg-sweep-v" + apiVersion)
                    .setProducerId(0L)
                    .setProducerEpoch((short) 0);

            client.getSync(new Request(ApiKeys.ADD_OFFSETS_TO_TXN, apiVersion,
                    "test-client", request));
        }

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.ADD_OFFSETS_TO_TXN))
                .as("v%d ADD_OFFSETS_TO_TXN should route to route-b for bob", apiVersion)
                .isNotEmpty();
    }

    static List<Arguments> txnOffsetCommitVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 0; v <= ApiKeys.TXN_OFFSET_COMMIT.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    /**
     * Sweeps TXN_OFFSET_COMMIT versions. The request will fail (no active
     * transaction) but the router should direct to route-b for bob.
     */
    @ParameterizedTest
    @MethodSource("txnOffsetCommitVersions")
    void txnOffsetCommitAcrossVersions(
                                       short apiVersion,
                                       @SaslMechanism(value = "PLAIN", principals = {
                                               @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                       }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                       @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {

            negotiateApiVersions(client);
            authenticate(client, "bob", "bob-secret");

            var request = new TxnOffsetCommitRequestData()
                    .setTransactionalId("txn-sweep-commit-v" + apiVersion)
                    .setGroupId("cg-sweep-v" + apiVersion)
                    .setProducerId(0L)
                    .setProducerEpoch((short) 0);

            client.getSync(new Request(ApiKeys.TXN_OFFSET_COMMIT, apiVersion,
                    "test-client", request));
        }

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.TXN_OFFSET_COMMIT))
                .as("v%d TXN_OFFSET_COMMIT should route to route-b for bob", apiVersion)
                .isNotEmpty();
    }

    // --- protocol helpers ---

    private static void negotiateApiVersions(KafkaClient client) {
        client.getSync(new Request(
                ApiKeys.API_VERSIONS,
                ApiKeys.API_VERSIONS.latestVersion(),
                "test-client",
                new ApiVersionsRequestData()
                        .setClientSoftwareName("test")
                        .setClientSoftwareVersion("1.0")));
    }

    private static void authenticate(KafkaClient client,
                                     String username,
                                     String password) {
        var handshakeResp = (SaslHandshakeResponseData) client.getSync(new Request(
                ApiKeys.SASL_HANDSHAKE,
                ApiKeys.SASL_HANDSHAKE.latestVersion(),
                "test-client",
                new SaslHandshakeRequestData().setMechanism("PLAIN")))
                .payload().message();
        assertThat(Errors.forCode(handshakeResp.errorCode())).isEqualTo(Errors.NONE);

        byte[] saslBytes = (username + "\0" + username + "\0" + password)
                .getBytes(StandardCharsets.UTF_8);
        var authResp = (SaslAuthenticateResponseData) client.getSync(new Request(
                ApiKeys.SASL_AUTHENTICATE,
                ApiKeys.SASL_AUTHENTICATE.latestVersion(),
                "test-client",
                new SaslAuthenticateRequestData().setAuthBytes(saslBytes)))
                .payload().message();
        assertThat(Errors.forCode(authResp.errorCode())).isEqualTo(Errors.NONE);
    }

    @SuppressWarnings("unchecked")
    private static <T extends ApiMessage> T awaitSuccessfulResponse(
                                                                    KafkaClient client,
                                                                    ApiKeys apiKey,
                                                                    short apiVersion,
                                                                    ApiMessage request,
                                                                    Class<T> responseClass,
                                                                    ToIntFunction<T> errorCodeExtractor) {
        var holder = new Object() {
            T value;
        };
        Awaitility.await()
                .atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    var response = client.getSync(
                            new Request(apiKey, apiVersion, "test-client", request));
                    holder.value = responseClass.cast(response.payload().message());
                    assertThat(errorCodeExtractor.applyAsInt(holder.value))
                            .isEqualTo(Errors.NONE.code());
                });
        return holder.value;
    }

    // --- config helpers ---

    private ConfigurationBuilder buildConfig(KafkaCluster clusterA,
                                             KafkaCluster clusterB,
                                             Map<String, String> subjectRoutes) {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", 0, null, new RouteDefinition.Target("cluster-a", null));
        var routeB = new RouteDefinition("route-b", 1, null, new RouteDefinition.Target("cluster-b", null));

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(
                        new RouteConfig("route-a", List.of("a."), null,
                                subjectsForRoute("route-a", subjectRoutes)),
                        new RouteConfig("route-b", List.of("b."), null,
                                subjectsForRoute("route-b", subjectRoutes))));

        var routerDef = new RouterDefinition("topic-router",
                TopicPartitionRouterFactory.class.getName(), routerConfig, List.of(routeA, routeB));

        var saslFilter = new NamedFilterDefinitionBuilder(
                SaslInspection.class.getName(),
                SaslInspection.class.getName())
                .withConfig("enabledMechanisms", Set.of("PLAIN"))
                .build();

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
                .addToFilterDefinitions(saslFilter)
                .addToDefaultFilters(saslFilter.name())
                .addToVirtualClusters(vc);
    }

    private static List<String> subjectsForRoute(String route,
                                                 Map<String, String> subjectRoutes) {
        var subjects = subjectRoutes.entrySet().stream()
                .filter(e -> route.equals(e.getValue()))
                .map(Map.Entry::getKey)
                .toList();
        return subjects.isEmpty() ? null : subjects;
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

    private static List<ConsumerRecord<String, String>> consumeDirectly(
                                                                        KafkaCluster cluster,
                                                                        String topic,
                                                                        String isolationLevel) {
        var config = new HashMap<>(cluster.getKafkaClientConfiguration());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + System.nanoTime());
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel);

        var records = new ArrayList<ConsumerRecord<String, String>>();
        try (var consumer = new KafkaConsumer<String, String>(config)) {
            consumer.subscribe(List.of(topic));
            Awaitility.await()
                    .atMost(Duration.ofSeconds(15))
                    .pollInterval(Duration.ofMillis(500))
                    .until(() -> {
                        consumer.poll(Duration.ofMillis(500)).forEach(records::add);
                        return !records.isEmpty();
                    });
        }
        catch (ConditionTimeoutException e) {
        }
        return records;
    }
}
