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
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.message.ApiVersionsRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.SaslAuthenticateRequestData;
import org.apache.kafka.common.message.SaslAuthenticateResponseData;
import org.apache.kafka.common.message.SaslHandshakeRequestData;
import org.apache.kafka.common.message.SaslHandshakeResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.filter.sasl.inspection.SaslInspection;
import io.kroxylicious.proxy.config.ConfigurationBuilder;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.RouteDefinition;
import io.kroxylicious.proxy.config.RouteTarget;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.VirtualClusterBuilder;
import io.kroxylicious.proxy.router.topic.TopicPartitionRouterFactory;
import io.kroxylicious.proxy.router.topic.config.RouteConfig;
import io.kroxylicious.proxy.router.topic.config.TopicPartitionRouterConfig;
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
 * Integration tests for consumer group router through the topic-partition
 * router using subject-based consumer group route mapping and the new
 * consumer group protocol (KIP-848).
 */
@ExtendWith(KafkaClusterExtension.class)
@ExtendWith(NettyLeakDetectorExtension.class)
class ConsumerGroupRoutingIT {

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
     * User "bob" is mapped to route-b in {@code consumerGroupUserRoutes}.
     * A producer writes to a {@code b.*} topic on clusterB, then "bob"
     * consumes from that topic using the new consumer group protocol.
     * The consumer's group coordinator is discovered on route-b.
     */
    @Test
    void shouldConsumeAndCommitOnMappedRoute(
                                             @SaslMechanism(value = "PLAIN", principals = {
                                                     @SaslMechanism.Principal(user = "alice", password = "alice-secret"),
                                                     @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                             }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                             @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "b.cg-mapped";
        createTopicOnCluster(topic, 1, clusterB);
        produceDirectly(clusterB, topic, "key1", "val1");

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            var consumerConfig = saslConsumerConfig(
                    tester.getBootstrapAddress(), "bob", "bob-secret", "cg-bob-mapped");

            try (var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
                consumer.subscribe(List.of(topic));

                var records = pollUntilRecords(consumer, Duration.ofSeconds(30));
                assertThat(records).hasSize(1);
                assertThat(records.iterator().next().value()).isEqualTo("val1");

                consumer.commitSync(Duration.ofSeconds(10));
            }
        }

        assertGroupExistsOnCluster("cg-bob-mapped", clusterB);
        assertGroupAbsentFromCluster("cg-bob-mapped", clusterA);

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.FIND_COORDINATOR))
                .as("FIND_COORDINATOR should be routed to route-b for bob")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("CONSUMER_GROUP_HEARTBEAT should be routed to route-b for bob")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("no heartbeats should go to route-a")
                .isEmpty();
    }

    /**
     * User "alice" has no entry in {@code consumerGroupUserRoutes}, so her
     * group coordinator is discovered on the default route (route-a / clusterA).
     * A producer writes to an {@code a.*} topic on clusterA, then "alice"
     * consumes from that topic.
     */
    @Test
    void shouldConsumeOnDefaultRoute(
                                     @SaslMechanism(value = "PLAIN", principals = {
                                             @SaslMechanism.Principal(user = "alice", password = "alice-secret"),
                                             @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                     }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                     @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "a.cg-default";
        createTopicOnCluster(topic, 1, clusterA);
        produceDirectly(clusterA, topic, "key1", "val1");

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            var consumerConfig = saslConsumerConfig(
                    tester.getBootstrapAddress(), "alice", "alice-secret", "cg-alice-default");

            try (var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
                consumer.subscribe(List.of(topic));

                var records = pollUntilRecords(consumer, Duration.ofSeconds(30));
                assertThat(records).hasSize(1);
                assertThat(records.iterator().next().value()).isEqualTo("val1");
            }
        }

        assertGroupExistsOnCluster("cg-alice-default", clusterA);
        assertGroupAbsentFromCluster("cg-alice-default", clusterB);

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("CONSUMER_GROUP_HEARTBEAT should be routed to route-a for unmapped alice")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("no heartbeats should go to route-b")
                .isEmpty();
    }

    /**
     * After "bob" commits offsets on the mapped route (route-b), a second consumer
     * in the same group should resume from the committed position, confirming that
     * offset storage landed on the correct cluster.
     */
    @Test
    void shouldFetchCommittedOffsetsOnMappedRoute(
                                                  @SaslMechanism(value = "PLAIN", principals = {
                                                          @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                                  }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                                  @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "b.cg-offsets";
        createTopicOnCluster(topic, 1, clusterB);
        produceDirectly(clusterB, topic, "k1", "first");
        produceDirectly(clusterB, topic, "k2", "second");

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            var consumerConfig = saslConsumerConfig(
                    tester.getBootstrapAddress(), "bob", "bob-secret", "cg-bob-offsets");

            try (var consumer = new KafkaConsumer<String, String>(consumerConfig)) {
                consumer.subscribe(List.of(topic));
                var records = pollUntilRecords(consumer, Duration.ofSeconds(30));
                assertThat(records).hasSizeGreaterThanOrEqualTo(1);
                consumer.commitSync(Duration.ofSeconds(10));
            }

            produceDirectly(clusterB, topic, "k3", "third");

            try (var consumer2 = new KafkaConsumer<String, String>(consumerConfig)) {
                consumer2.subscribe(List.of(topic));
                var records = pollUntilRecords(consumer2, Duration.ofSeconds(30));
                assertThat(records).allSatisfy(r -> assertThat(r.value()).as("should resume after committed offset")
                        .isNotEqualTo("first"));
            }
        }
    }

    /**
     * Both "alice" and "bob" are mapped to different routes. Each user
     * consumes from a topic on their respective cluster, verifying that
     * two independent consumer groups are created on separate clusters.
     */
    @Test
    void shouldRouteTwoMappedUsersToSeparateClusters(
                                                     @SaslMechanism(value = "PLAIN", principals = {
                                                             @SaslMechanism.Principal(user = "alice", password = "alice-secret"),
                                                             @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                                     }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                                     @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topicA = "a.cg-multi-alice";
        String topicB = "b.cg-multi-bob";
        createTopicOnCluster(topicA, 1, clusterA);
        createTopicOnCluster(topicB, 1, clusterB);
        produceDirectly(clusterA, topicA, "ka", "alice-val");
        produceDirectly(clusterB, topicB, "kb", "bob-val");

        var config = buildConfig(clusterA, clusterB,
                Map.of("alice", "route-a", "bob", "route-b"));

        try (var tester = kroxyliciousTester(config)) {
            var aliceConfig = saslConsumerConfig(
                    tester.getBootstrapAddress(), "alice", "alice-secret", "cg-alice-multi");
            var bobConfig = saslConsumerConfig(
                    tester.getBootstrapAddress(), "bob", "bob-secret", "cg-bob-multi");

            try (var aliceConsumer = new KafkaConsumer<String, String>(aliceConfig);
                    var bobConsumer = new KafkaConsumer<String, String>(bobConfig)) {
                aliceConsumer.subscribe(List.of(topicA));
                bobConsumer.subscribe(List.of(topicB));

                var aliceRecords = pollUntilRecords(aliceConsumer, Duration.ofSeconds(30));
                assertThat(aliceRecords).hasSize(1);
                assertThat(aliceRecords.iterator().next().value()).isEqualTo("alice-val");

                var bobRecords = pollUntilRecords(bobConsumer, Duration.ofSeconds(30));
                assertThat(bobRecords).hasSize(1);
                assertThat(bobRecords.iterator().next().value()).isEqualTo("bob-val");
            }
        }

        assertGroupExistsOnCluster("cg-alice-multi", clusterA);
        assertGroupAbsentFromCluster("cg-alice-multi", clusterB);
        assertGroupExistsOnCluster("cg-bob-multi", clusterB);
        assertGroupAbsentFromCluster("cg-bob-multi", clusterA);

        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("heartbeats for alice should go to route-a")
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("heartbeats for bob should go to route-b")
                .isNotEmpty();
    }

    // --- version sweep ---

    static List<Arguments> consumerGroupHeartbeatVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 0; v <= ApiKeys.CONSUMER_GROUP_HEARTBEAT.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    /**
     * Sweeps all supported CONSUMER_GROUP_HEARTBEAT versions via the raw
     * protocol client. Authenticates as "bob" (mapped to route-b), sends
     * a heartbeat with memberEpoch=0 (join), and verifies a valid response
     * and correct router.
     */
    @ParameterizedTest
    @MethodSource("consumerGroupHeartbeatVersions")
    void consumerGroupHeartbeatAcrossVersions(
                                              short apiVersion,
                                              @SaslMechanism(value = "PLAIN", principals = {
                                                      @SaslMechanism.Principal(user = "bob", password = "bob-secret")
                                              }) @BrokerCluster(numBrokers = 3) KafkaCluster clusterA,
                                              @BrokerCluster(numBrokers = 3) KafkaCluster clusterB)
            throws Exception {
        String topic = "b.cg-sweep-v" + apiVersion;
        createTopicOnCluster(topic, 1, clusterB);

        var config = buildConfig(clusterA, clusterB, Map.of("bob", "route-b"));

        try (var tester = kroxyliciousTester(config);
                var client = tester.simpleTestClient()) {

            negotiateApiVersions(client);
            authenticate(client, "bob", "bob-secret");

            var heartbeatReq = new ConsumerGroupHeartbeatRequestData()
                    .setGroupId("cg-sweep-v" + apiVersion)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(60000)
                    .setSubscribedTopicNames(List.of(topic))
                    .setTopicPartitions(List.of());
            if (apiVersion >= 1) {
                heartbeatReq.setMemberId(java.util.UUID.randomUUID().toString());
            }

            client.getSync(new Request(ApiKeys.CONSUMER_GROUP_HEARTBEAT, apiVersion,
                    "test-client", heartbeatReq));
        }

        assertThat(routingCaptor.requestsToRoute("route-b", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("v%d heartbeat should route to route-b", apiVersion)
                .isNotEmpty();
        assertThat(routingCaptor.requestsToRoute("route-a", ApiKeys.CONSUMER_GROUP_HEARTBEAT))
                .as("v%d no heartbeats should go to route-a", apiVersion)
                .isEmpty();
    }

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
     * keyType=0 (consumer group). Verifies router to the mapped route.
     */
    @ParameterizedTest
    @MethodSource("findCoordinatorVersions")
    void findCoordinatorForGroupAcrossVersions(
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
                    .setKey("cg-find-v" + apiVersion)
                    .setKeyType((byte) 0);

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

    // --- config helpers ---

    private ConfigurationBuilder buildConfig(KafkaCluster clusterA,
                                             KafkaCluster clusterB,
                                             Map<String, String> subjectRoutes) {
        var targetA = new ClusterDefinition("cluster-a", clusterA.getBootstrapServers(), null);
        var targetB = new ClusterDefinition("cluster-b", clusterB.getBootstrapServers(), null);

        var routeA = new RouteDefinition("route-a", null, new RouteTarget("cluster-a", null));
        var routeB = new RouteDefinition("route-b", null, new RouteTarget("cluster-b", null));

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
                .withTarget(new RouteTarget(null, "topic-router"))
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

    private static Map<String, Object> saslConsumerConfig(String bootstrap,
                                                          String username,
                                                          String password,
                                                          String groupId) {
        var config = new HashMap<String, Object>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        config.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        config.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule required\n"
                        + "  username=\"" + username + "\"\n"
                        + "  password=\"" + password + "\";");
        return config;
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

    private static void produceDirectly(KafkaCluster cluster,
                                        String topic,
                                        String key,
                                        String value)
            throws Exception {
        var config = new HashMap<>(cluster.getKafkaClientConfiguration());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (var producer = new KafkaProducer<String, String>(config)) {
            producer.send(new ProducerRecord<>(topic, key, value)).get(10, TimeUnit.SECONDS);
        }
    }

    private static void assertGroupExistsOnCluster(String groupId,
                                                   KafkaCluster cluster)
            throws Exception {
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            var groups = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            assertThat(groups)
                    .extracting(org.apache.kafka.clients.admin.ConsumerGroupListing::groupId)
                    .as("group %s should exist on cluster %s", groupId, cluster.getBootstrapServers())
                    .contains(groupId);
        }
    }

    private static void assertGroupAbsentFromCluster(String groupId,
                                                     KafkaCluster cluster)
            throws Exception {
        try (var admin = AdminClient.create(cluster.getKafkaClientConfiguration())) {
            var groups = admin.listConsumerGroups().all().get(10, TimeUnit.SECONDS);
            assertThat(groups)
                    .extracting(org.apache.kafka.clients.admin.ConsumerGroupListing::groupId)
                    .as("group %s should not exist on cluster %s", groupId, cluster.getBootstrapServers())
                    .doesNotContain(groupId);
        }
    }

    private static <K, V> ConsumerRecords<K, V> pollUntilRecords(KafkaConsumer<K, V> consumer,
                                                                 Duration timeout) {
        var result = new AtomicReference<>(ConsumerRecords.<K, V> empty());
        Awaitility.await()
                .atMost(timeout)
                .pollInterval(Duration.ofMillis(500))
                .until(() -> {
                    var records = consumer.poll(Duration.ofMillis(500));
                    if (!records.isEmpty()) {
                        result.set(records);
                    }
                    return !result.get().isEmpty();
                });
        return result.get();
    }
}
