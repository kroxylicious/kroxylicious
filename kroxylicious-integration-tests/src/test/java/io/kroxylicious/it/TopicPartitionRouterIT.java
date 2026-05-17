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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.it.testplugins.FaultInjectionFilterFactory;
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
import io.kroxylicious.testing.kafka.common.BrokerCluster;
import io.kroxylicious.testing.kafka.junit5ext.KafkaClusterExtension;

import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.baseConfigurationBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousConfigUtils.defaultPortIdentifiesNodeGatewayBuilder;
import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Integration tests for {@link TopicPartitionRouterFactory}.
 * Covers version capping (Phase 1), produce fan-out (Phase 2),
 * idempotent produce (Phase 3), and metadata merging (Phase 4).
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class TopicPartitionRouterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(TopicPartitionRouterIT.class);

    /**
     * METADATA version used for raw protocol tests. v9 is the first flexible
     * version and avoids the nullable-name semantics introduced at v12+.
     */
    private static final short METADATA_VERSION = 9;

    static KafkaCluster clusterA;
    static KafkaCluster clusterB;

    /** Request shapes for the parameterised metadata test. */
    enum MetadataShape {
        /** All topics (topics = null, v1+). */
        ALL_TOPICS,
        /** Broker info only (topics = empty list, v4+). */
        BROKER_INFO_ONLY,
        /** Single route — topic on default route only. */
        SINGLE_ROUTE,
        /** Cross route — topics on both routes, requires fan-out. */
        CROSS_ROUTE
    }

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

    private RoutingEventCaptor routingCaptor;

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
    }

    private static void createTopicOnCluster(String topicName,
                                             int partitions,
                                             KafkaCluster cluster)
            throws Exception {
        var newTopic = new NewTopic(topicName, Optional.of(partitions), Optional.empty());
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            admin.createTopics(List.of(newTopic)).all().get(10, TimeUnit.SECONDS);
        }
    }

    private void createTopic(String topicName, KafkaCluster... clusters) throws Exception {
        for (var cluster : clusters) {
            createTopicOnCluster(topicName, 1, cluster);
        }
    }

    private ConfigurationBuilder topicRouterConfig() {
        return topicRouterConfig(clusterA, clusterB);
    }

    private ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                                   KafkaCluster b) {
        return topicRouterConfig(a, b, null);
    }

    private ConfigurationBuilder topicRouterConfig(KafkaCluster a,
                                                   KafkaCluster b,
                                                   Duration producerIdTtl) {
        var targetA = new TargetClusterDefinition("cluster-a", a.getBootstrapServers(), null);
        var targetB = new TargetClusterDefinition("cluster-b", b.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", null, "cluster-a", null);
        var routeB = new RouteDefinition("route-b", null, "cluster-b", null);

        var routerConfig = new TopicPartitionRouterConfig(
                "route-a",
                List.of(
                        new TopicRoute("route-a", List.of("a.")),
                        new TopicRoute("route-b", List.of("b."))),
                producerIdTtl);

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

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.API_VERSIONS))
                .as("API_VERSIONS should be routed to default route")
                .hasSize(1);
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
                .containsExactly("val-0", "val-1", "val-2", "val-3", "val-4");

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("all produces should go to route-a")
                .hasSize(5);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("no produces should go to route-b")
                .isEmpty();
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
                .containsExactly("val-a1", "val-a2");

        assertThat(recordsB).hasSize(1);
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("val-b1");

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("a.orders produces to route-a")
                .hasSize(2);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("b.logs produces to route-b")
                .hasSize(1);
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

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("fire-and-forget produce to route-a")
                .hasSize(1);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .as("fire-and-forget produce to route-b")
                .hasSize(1);
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

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("unprefixed topic should route to default route")
                .hasSize(1);
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE))
                .isEmpty();
    }

    // --- Phase 3: idempotent produce ---

    @Test
    void shouldProduceWithIdempotenceToMultipleRoutes() throws Exception {
        String topicA = "a.idempotent";
        String topicB = "b.idempotent";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        // Burn a producer ID on clusterA so the two clusters allocate different PIDs,
        // making the per-route assertions non-vacuous.
        try (var warmup = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true))) {
            warmup.send(new ProducerRecord<>(topicA, "warmup", "warmup"))
                    .get(10, TimeUnit.SECONDS);
        }

        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 3,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicA, "key-a2", "val-a2"))
                    .get(10, TimeUnit.SECONDS);
        }

        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);

        assertThat(recordsA).hasSize(3);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("warmup", "val-a", "val-a2");

        assertThat(recordsB).hasSize(1);
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("val-b");

        // INIT_PRODUCER_ID fanned out to both routes
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.INIT_PRODUCER_ID))
                .as("INIT_PRODUCER_ID to route-a")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.INIT_PRODUCER_ID))
                .as("INIT_PRODUCER_ID to route-b")
                .isNotEmpty();

        // Extract the producer ID each cluster allocated from INIT_PRODUCER_ID responses
        long allocatedPidA = initProducerIdFromResponse(routingCaptor, "route-a");
        long allocatedPidB = initProducerIdFromResponse(routingCaptor, "route-b");
        assertThat(allocatedPidA).as("route-a producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(allocatedPidB).as("route-b producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(allocatedPidA)
                .as("routes should have different producer IDs to prove per-route mapping")
                .isNotEqualTo(allocatedPidB);

        // PRODUCE to each route uses the route-specific producer ID
        var produceToA = routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE);
        var produceToB = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);
        assertThat(produceToA).hasSize(2);
        assertThat(produceToB).hasSize(1);

        produceToA.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(allocatedPidA));
        produceToB.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(allocatedPidB));

        // Sequence numbers per route start from 0 and increment
        RoutingRequestEventAssert.assertThat(produceToA.get(0)).hasBaseSequence(0);
        RoutingRequestEventAssert.assertThat(produceToA.get(1)).hasBaseSequence(1);
        RoutingRequestEventAssert.assertThat(produceToB.get(0)).hasBaseSequence(0);
    }

    @Test
    void shouldProduceWithIdempotenceToNonDefaultRoute() throws Exception {
        String topicB = "b.idem-nondefault";
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 3,
                        "batch.size", 0,
                        "linger.ms", 0))) {
            for (int i = 0; i < 5; i++) {
                producer.send(new ProducerRecord<>(topicB, "key-" + i, "val-" + i))
                        .get(10, TimeUnit.SECONDS);
            }
        }

        var records = consumeDirectly(clusterB, topicB);
        assertThat(records).hasSize(5);
        assertThat(records).extracting(ConsumerRecord::value)
                .containsExactly("val-0", "val-1", "val-2", "val-3", "val-4");

        // All produces to route-b with consistent producer ID
        var produces = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);
        assertThat(produces).hasSize(5);

        long pid = produces.get(0).firstProducerId().orElseThrow();
        produces.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pid));

        // Sequence numbers increment from 0
        for (int i = 0; i < produces.size(); i++) {
            RoutingRequestEventAssert.assertThat(produces.get(i)).hasBaseSequence(i);
        }
    }

    // --- Fault injection ---

    @Test
    void shouldDisconnectClientWhenFaultInjected() throws Exception {
        String topicA = "a.fault";
        createTopic(topicA, clusterA);

        FaultInjectionFilterFactory.reset();
        var config = topicRouterConfig()
                .addNewFilterDefinition(
                        "fault",
                        FaultInjectionFilterFactory.class.getName(),
                        null)
                .addToDefaultFilters("fault");

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", false,
                        "retries", 0,
                        "batch.size", 0,
                        "linger.ms", 0,
                        "request.timeout.ms", 5000,
                        "delivery.timeout.ms", 10000))) {

            producer.send(new ProducerRecord<>(topicA, "before", "ok"))
                    .get(10, TimeUnit.SECONDS);

            var records = consumeDirectly(clusterA, topicA);
            assertThat(records).hasSize(1);
            assertThat(records.get(0).value()).isEqualTo("ok");

            FaultInjectionFilterFactory.latestHandle().closeOnNextRequest();

            assertThat(catchThrowable(() -> producer.send(new ProducerRecord<>(topicA, "after", "should-fail"))
                    .get(10, TimeUnit.SECONDS)))
                    .as("produce should fail after fault injection")
                    .isInstanceOf(ExecutionException.class);
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE))
                .as("at least one produce should have reached route-a before fault")
                .isNotEmpty();
    }

    @Test
    void shouldPreserveIdempotencyAcrossDisconnect() throws Exception {
        String topicA = "a.idem-reconnect";
        String topicB = "b.idem-reconnect";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        // Burn a PID on clusterA so the two clusters allocate different PIDs
        try (var warmup = new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName(),
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true))) {
            warmup.send(new ProducerRecord<>(topicA, "warmup", "warmup"))
                    .get(10, TimeUnit.SECONDS);
        }

        FaultInjectionFilterFactory.reset();
        var config = topicRouterConfig()
                .addNewFilterDefinition(
                        "fault",
                        FaultInjectionFilterFactory.class.getName(),
                        null)
                .addToDefaultFilters("fault");

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 5,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            // Phase 1: produce successfully before disconnect
            producer.send(new ProducerRecord<>(topicA, "k1", "before-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k1", "before-b1"))
                    .get(10, TimeUnit.SECONDS);

            // Phase 2: inject fault — producer disconnects and auto-reconnects
            FaultInjectionFilterFactory.latestHandle().closeOnNextRequest();
            producer.send(new ProducerRecord<>(topicA, "k2", "reconnect-a"))
                    .get(30, TimeUnit.SECONDS);

            // Phase 3: produce more after reconnect
            producer.send(new ProducerRecord<>(topicA, "k3", "after-a1"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k2", "after-b1"))
                    .get(10, TimeUnit.SECONDS);
        }

        // Black-box: no duplicates on either backend, in order
        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("warmup", "before-a1", "reconnect-a", "after-a1");
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("before-b1", "after-b1");

        // White-box: per-route PIDs are consistent across the disconnect
        long pidA = initProducerIdFromResponse(routingCaptor, "route-a");
        long pidB = initProducerIdFromResponse(routingCaptor, "route-b");
        assertThat(pidA).as("route-a producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(pidB).as("route-b producer ID").isNotEqualTo(RecordBatch.NO_PRODUCER_ID);
        assertThat(pidA)
                .as("routes should have different producer IDs")
                .isNotEqualTo(pidB);

        var producesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.PRODUCE);
        var producesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.PRODUCE);

        // All produces to each route use the same PID — proves mapping survived reconnect
        producesToA.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pidA));
        producesToB.forEach(event -> RoutingRequestEventAssert.assertThat(event).hasProducerId(pidB));
    }

    @Test
    void shouldRecoverAfterProducerIdEviction() throws Exception {
        String topicA = "a.eviction";
        String topicB = "b.eviction";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        var config = topicRouterConfig(clusterA, clusterB, Duration.ofSeconds(2));

        try (var tester = kroxyliciousTester(config);
                var producer = tester.producer(Map.of(
                        "enable.idempotence", true,
                        "retries", 5,
                        "batch.size", 0,
                        "linger.ms", 0))) {

            // Phase 1: produce to both routes — establishes per-route PID mappings
            producer.send(new ProducerRecord<>(topicA, "k1", "before-a"))
                    .get(10, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k1", "before-b"))
                    .get(10, TimeUnit.SECONDS);

            // Phase 2: wait for TTL to expire
            Thread.sleep(3000);

            // Phase 3: produce again — mapping evicted, producer re-initialises and retries
            producer.send(new ProducerRecord<>(topicA, "k2", "after-a"))
                    .get(30, TimeUnit.SECONDS);
            producer.send(new ProducerRecord<>(topicB, "k2", "after-b"))
                    .get(30, TimeUnit.SECONDS);
        }

        // No duplicates on either backend, in order
        var recordsA = consumeDirectly(clusterA, topicA);
        var recordsB = consumeDirectly(clusterB, topicB);
        assertThat(recordsA).extracting(ConsumerRecord::value)
                .containsExactly("before-a", "after-a");
        assertThat(recordsB).extracting(ConsumerRecord::value)
                .containsExactly("before-b", "after-b");
    }

    // --- Phase 4: metadata merging ---

    @Test
    void shouldReturnMetadataForTopicsOnBothRoutes() throws Exception {
        String topicA = "a.meta";
        String topicB = "b.meta";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            negotiateApiVersions(client);
            var request = new MetadataRequestData();
            request.topics().add(new MetadataRequestTopic().setName(topicA));
            request.topics().add(new MetadataRequestTopic().setName(topicB));

            var response = client.getSync(
                    new Request(ApiKeys.METADATA, METADATA_VERSION, "test-client", request));
            var body = (MetadataResponseData) response.payload().message();

            assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                    .containsExactlyInAnyOrder(topicA, topicB);

            for (var t : body.topics()) {
                assertThat(t.partitions())
                        .as("topic %s (error=%s) should have partitions",
                                t.name(), Errors.forCode(t.errorCode()))
                        .isNotEmpty();
            }
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.METADATA))
                .as("METADATA for a.meta should be routed to route-a")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.METADATA))
                .as("METADATA for b.meta should be routed to route-b")
                .isNotEmpty();
    }

    @Test
    void shouldReturnBrokerListInMergedMetadata() throws Exception {
        String topicA = "a.brokers";
        String topicB = "b.brokers";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            negotiateApiVersions(client);
            var request = new MetadataRequestData();
            request.topics().add(new MetadataRequestTopic().setName(topicA));
            request.topics().add(new MetadataRequestTopic().setName(topicB));

            var response = client.getSync(
                    new Request(ApiKeys.METADATA, METADATA_VERSION, "test-client", request));
            var body = (MetadataResponseData) response.payload().message();

            // Both single-node test clusters have nodeId 0, so after proxy address
            // remapping the union has one entry. With multi-node clusters the
            // union would be larger.
            assertThat(body.brokers())
                    .as("merged broker list should contain at least one broker")
                    .isNotEmpty();

            assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                    .as("both topics should be present in merged response")
                    .containsExactlyInAnyOrder(topicA, topicB);
        }
    }

    @Test
    void shouldExcludePhantomTopicsFromAllTopicsMetadata() throws Exception {
        String topicA = "a.real-topic";
        String topicB = "b.real-topic";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            negotiateApiVersions(client);
            var request = new MetadataRequestData().setTopics(null);

            var response = client.getSync(
                    new Request(ApiKeys.METADATA, METADATA_VERSION, "test-client", request));
            var body = (MetadataResponseData) response.payload().message();

            assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                    .as("all-topics metadata should include topics from both routes")
                    .contains(topicA, topicB);

            // Phantom topics would be topics appearing on the wrong cluster
            // (e.g. "b.real-topic" on clusterA). The router filters these out.
            for (var topic : body.topics()) {
                if (topic.name() != null && topic.name().startsWith("b.")) {
                    assertThat(topic.partitions())
                            .as("b.* topic %s should have partition metadata from route-b", topic.name())
                            .isNotEmpty();
                }
            }
        }

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.METADATA))
                .as("all-topics METADATA should fan out to route-a")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.METADATA))
                .as("all-topics METADATA should fan out to route-b")
                .isNotEmpty();
    }

    @Test
    void shouldMergeBrokersFromMultiBrokerClusters(
                                                   @BrokerCluster(numBrokers = 3) KafkaCluster multiBrokerB)
            throws Exception {
        String topicA = "a.multi";
        String topicB = "b.multi";
        createTopicOnCluster(topicA, 1, clusterA);
        createTopicOnCluster(topicB, 3, multiBrokerB);
        var config = topicRouterConfig(clusterA, multiBrokerB);

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            negotiateApiVersions(client);
            var request = new MetadataRequestData();
            request.topics().add(new MetadataRequestTopic().setName(topicA));
            request.topics().add(new MetadataRequestTopic().setName(topicB));

            var response = client.getSync(
                    new Request(ApiKeys.METADATA, METADATA_VERSION, "test-client", request));
            var body = (MetadataResponseData) response.payload().message();

            // clusterA has nodeId 0; multiBrokerB has nodeIds 0, 1, 2.
            // After proxy address remapping the union deduplicates by nodeId,
            // giving 3 distinct broker entries.
            assertThat(body.brokers())
                    .as("merged broker list should contain brokers from both clusters")
                    .hasSizeGreaterThanOrEqualTo(3);

            assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                    .containsExactlyInAnyOrder(topicA, topicB);

            var topicBMeta = body.topics().stream()
                    .filter(t -> t.name().equals(topicB))
                    .findFirst()
                    .orElseThrow();
            assertThat(topicBMeta.partitions())
                    .as("b.multi should have 3 partitions spread across the multi-broker cluster")
                    .hasSize(3);
        }
    }

    // --- Phase 4: parameterised metadata version × shape ---

    static List<Arguments> metadataVersionsAndShapes() {
        List<Arguments> result = new ArrayList<>();
        for (short version : ApiKeys.METADATA.allVersions()) {
            for (MetadataShape shape : MetadataShape.values()) {
                if (shape == MetadataShape.ALL_TOPICS && version < 1) {
                    continue;
                }
                if (shape == MetadataShape.BROKER_INFO_ONLY && version < 4) {
                    continue;
                }
                result.add(Arguments.of(version, shape));
            }
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("metadataVersionsAndShapes")
    void metadataAcrossVersionsAndShapes(short apiVersion,
                                         MetadataShape shape)
            throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {
            negotiateApiVersions(client);

            var request = new MetadataRequestData();
            switch (shape) {
                case ALL_TOPICS -> request.setTopics(null);
                case BROKER_INFO_ONLY -> {
                    /* empty topics list is the default */ }
                case SINGLE_ROUTE -> request.topics().add(new MetadataRequestTopic().setName(PARAM_TOPIC_A));
                case CROSS_ROUTE -> {
                    request.topics().add(new MetadataRequestTopic().setName(PARAM_TOPIC_A));
                    request.topics().add(new MetadataRequestTopic().setName(PARAM_TOPIC_B));
                }
            }

            var response = client.getSync(
                    new Request(ApiKeys.METADATA, apiVersion, "test-client", request));
            var body = (MetadataResponseData) response.payload().message();

            assertThat(body.brokers())
                    .as("brokers should be present for v%d %s", apiVersion, shape)
                    .isNotEmpty();

            switch (shape) {
                case ALL_TOPICS -> {
                    assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                            .as("all-topics at v%d should include both test topics", apiVersion)
                            .contains(PARAM_TOPIC_A, PARAM_TOPIC_B);
                    for (var topic : body.topics()) {
                        if (topic.name() != null && topic.name().startsWith("b.")) {
                            assertThat(topic.partitions())
                                    .as("b.* topic %s should have partition metadata (not a phantom)", topic.name())
                                    .isNotEmpty();
                        }
                    }
                }
                case BROKER_INFO_ONLY -> {
                    assertThat(body.topics())
                            .as("broker-info-only at v%d should return no topics", apiVersion)
                            .isEmpty();
                }
                case SINGLE_ROUTE -> {
                    assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                            .as("single-route at v%d should return only a.param", apiVersion)
                            .containsExactly(PARAM_TOPIC_A);
                    var singleTopic = body.topics().iterator().next();
                    assertThat(singleTopic.errorCode())
                            .as("a.param should have no error at v%d", apiVersion)
                            .isEqualTo(Errors.NONE.code());
                    assertThat(singleTopic.partitions())
                            .as("a.param should have partitions at v%d", apiVersion)
                            .isNotEmpty();
                }
                case CROSS_ROUTE -> {
                    assertThat(body.topics()).extracting(MetadataResponseTopic::name)
                            .as("cross-route at v%d should return both topics", apiVersion)
                            .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);
                    for (var t : body.topics()) {
                        assertThat(t.errorCode())
                                .as("topic %s should have no error at v%d", t.name(), apiVersion)
                                .isEqualTo(Errors.NONE.code());
                        assertThat(t.partitions())
                                .as("topic %s should have partitions at v%d", t.name(), apiVersion)
                                .isNotEmpty();
                    }
                }
            }
        }

        switch (shape) {
            case ALL_TOPICS, CROSS_ROUTE -> {
                assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.METADATA))
                        .as("%s at v%d should route to route-a", shape, apiVersion)
                        .isNotEmpty();
                assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.METADATA))
                        .as("%s at v%d should route to route-b", shape, apiVersion)
                        .isNotEmpty();
            }
            case BROKER_INFO_ONLY, SINGLE_ROUTE -> {
                assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.METADATA))
                        .as("%s at v%d should route to default route-a", shape, apiVersion)
                        .isNotEmpty();
            }
        }
    }

    // --- Phase 5: fetch, list offsets, offset commit ---

    @Test
    void shouldFetchFromTopicsOnBothRoutes() throws Exception {
        String topicA = "a.fetch";
        String topicB = "b.fetch";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(topicA, "key-a", "val-a"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topicB, "key-b", "val-b"))
                        .get(10, TimeUnit.SECONDS);
            }

            var consumerProps = new java.util.HashMap<String, Object>();
            consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, tester.getBootstrapAddress());
            consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

            try (var consumer = new KafkaConsumer<String, String>(consumerProps)) {
                consumer.assign(List.of(
                        new TopicPartition(topicA, 0),
                        new TopicPartition(topicB, 0)));

                List<ConsumerRecord<String, String>> all = new ArrayList<>();
                long deadline = System.currentTimeMillis() + 10_000;
                while (all.size() < 2 && System.currentTimeMillis() < deadline) {
                    consumer.poll(Duration.ofMillis(500)).forEach(all::add);
                }

                assertThat(all).extracting(ConsumerRecord::value)
                        .containsExactlyInAnyOrder("val-a", "val-b");
            }
        }

        var fetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
        var fetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
        assertThat(fetchesToA).as("FETCH should be routed to route-a").isNotEmpty();
        assertThat(fetchesToB).as("FETCH should be routed to route-b").isNotEmpty();

        // Sessions must be forced off on every sub-request
        for (var event : fetchesToA) {
            var body = (FetchRequestData) event.body();
            assertThat(body.sessionId()).as("sessionId forced to 0 on route-a").isEqualTo(0);
            assertThat(body.sessionEpoch()).as("sessionEpoch forced to -1 on route-a").isEqualTo(-1);
            assertThat(body.forgottenTopicsData()).as("no forgotten topics on route-a").isEmpty();
            assertThat(body.topics()).extracting("topic")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : fetchesToB) {
            var body = (FetchRequestData) event.body();
            assertThat(body.sessionId()).as("sessionId forced to 0 on route-b").isEqualTo(0);
            assertThat(body.sessionEpoch()).as("sessionEpoch forced to -1 on route-b").isEqualTo(-1);
            assertThat(body.forgottenTopicsData()).as("no forgotten topics on route-b").isEmpty();
            assertThat(body.topics()).extracting("topic")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    @Test
    void shouldListOffsetsForTopicsOnBothRoutes() throws Exception {
        String topicA = "a.listoffsets";
        String topicB = "b.listoffsets";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(topicA, "k", "v"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(topicB, "k", "v"))
                        .get(10, TimeUnit.SECONDS);
            }

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new ListOffsetsRequestData()
                        .setReplicaId(-1)
                        .setIsolationLevel((byte) 0);
                var topicAReq = new ListOffsetsTopic().setName(topicA);
                topicAReq.partitions().add(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(-1));
                var topicBReq = new ListOffsetsTopic().setName(topicB);
                topicBReq.partitions().add(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(-1));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.LIST_OFFSETS, (short) 7, "test-client", request));
                var body = (ListOffsetsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(ListOffsetsTopicResponse::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.topics()) {
                    var partition = topic.partitions().get(0);
                    assertThat(partition.errorCode())
                            .as("partition error for %s", topic.name())
                            .isEqualTo(Errors.NONE.code());
                    assertThat(partition.offset())
                            .as("latest offset for %s", topic.name())
                            .isGreaterThanOrEqualTo(1);
                }
            }
        }

        var listOffsetsToA = routingCaptor.requestsToRoute("route-a", ApiKeys.LIST_OFFSETS);
        var listOffsetsToB = routingCaptor.requestsToRoute("route-b", ApiKeys.LIST_OFFSETS);
        assertThat(listOffsetsToA).as("LIST_OFFSETS should be routed to route-a").isNotEmpty();
        assertThat(listOffsetsToB).as("LIST_OFFSETS should be routed to route-b").isNotEmpty();

        for (var event : listOffsetsToA) {
            var body = (ListOffsetsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : listOffsetsToB) {
            var body = (ListOffsetsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    @Test
    void shouldCommitOffsetsForTopicsOnBothRoutes() throws Exception {
        String topicA = "a.commitoffsets";
        String topicB = "b.commitoffsets";
        String groupId = "test-simple-group";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

        // The proxy decomposes OFFSET_COMMIT by topic, sending sub-requests
        // to each route's cluster. Each cluster must have its coordinator
        // initialised for the group, which normally happens via
        // FIND_COORDINATOR. Warm up both clusters by committing a dummy
        // offset directly.
        warmUpGroupCoordinator(clusterA, topicA, groupId);
        warmUpGroupCoordinator(clusterB, topicB, groupId);

        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new OffsetCommitRequestData()
                        .setGroupId(groupId)
                        .setMemberId("")
                        .setGenerationIdOrMemberEpoch(-1);
                var topicAReq = new OffsetCommitRequestTopic().setName(topicA);
                topicAReq.partitions().add(new OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(42));
                var topicBReq = new OffsetCommitRequestTopic().setName(topicB);
                topicBReq.partitions().add(new OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(99));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.OFFSET_COMMIT, (short) 8, "test-client", request));
                var body = (OffsetCommitResponseData) response.payload().message();

                assertThat(body.topics()).extracting(OffsetCommitResponseTopic::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.topics()) {
                    assertThat(topic.partitions().get(0).errorCode())
                            .as("offset commit error for %s", topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        var commitsToA = routingCaptor.requestsToRoute("route-a", ApiKeys.OFFSET_COMMIT);
        var commitsToB = routingCaptor.requestsToRoute("route-b", ApiKeys.OFFSET_COMMIT);
        assertThat(commitsToA).as("OFFSET_COMMIT should be routed to route-a").isNotEmpty();
        assertThat(commitsToB).as("OFFSET_COMMIT should be routed to route-b").isNotEmpty();

        for (var event : commitsToA) {
            var body = (OffsetCommitRequestData) event.body();
            assertThat(body.groupId()).as("groupId preserved on route-a").isEqualTo(groupId);
            assertThat(body.topics()).extracting("name")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : commitsToB) {
            var body = (OffsetCommitRequestData) event.body();
            assertThat(body.groupId()).as("groupId preserved on route-b").isEqualTo(groupId);
            assertThat(body.topics()).extracting("name")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    private static void warmUpGroupCoordinator(KafkaCluster cluster,
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

    private static void negotiateApiVersions(KafkaClient client) {
        client.getSync(new Request(
                ApiKeys.API_VERSIONS,
                ApiKeys.API_VERSIONS.latestVersion(),
                "test-client",
                new ApiVersionsRequestData()
                        .setClientSoftwareName("test")
                        .setClientSoftwareVersion("1.0")));
    }

    private static long initProducerIdFromResponse(RoutingEventCaptor captor,
                                                   String route) {
        return captor.responseEvents().stream()
                .filter(e -> e.route().equals(route))
                .filter(e -> e.apiKey() == ApiKeys.INIT_PRODUCER_ID)
                .map(e -> (InitProducerIdResponseData) e.body())
                .filter(r -> Errors.forCode(r.errorCode()) == Errors.NONE)
                .mapToLong(InitProducerIdResponseData::producerId)
                .reduce((first, last) -> last)
                .orElseThrow(() -> new AssertionError(
                        "No successful INIT_PRODUCER_ID response captured for route " + route));
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
