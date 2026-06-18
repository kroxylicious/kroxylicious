/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.integration.Request;
import io.kroxylicious.testing.kafka.api.KafkaCluster;
import io.kroxylicious.testing.kafka.common.BrokerCluster;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for METADATA router through the topic-partition router.
 */
class MetadataRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final short METADATA_VERSION = 9;

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

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

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

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

    // --- Parameterised version x shape ---

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
}
