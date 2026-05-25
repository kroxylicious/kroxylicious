/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.integration.Request;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for LIST_OFFSETS router through the topic-partition router.
 */
class ListOffsetsRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
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

    // --- Version sweep ---

    static List<Arguments> listOffsetsVersions() {
        var result = new ArrayList<Arguments>();
        // Skip v0 — uses deprecated oldStyleOffsets response shape
        for (short v = 1; v <= ApiKeys.LIST_OFFSETS.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("listOffsetsVersions")
    void listOffsetsAcrossVersions(short apiVersion) throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            // Produce records so offsets are non-zero
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(PARAM_TOPIC_A, "k", "v"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(PARAM_TOPIC_B, "k", "v"))
                        .get(10, TimeUnit.SECONDS);
            }

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new ListOffsetsRequestData()
                        .setReplicaId(-1)
                        .setIsolationLevel((byte) 0);
                var topicAReq = new ListOffsetsTopic().setName(PARAM_TOPIC_A);
                topicAReq.partitions().add(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(-1));
                var topicBReq = new ListOffsetsTopic().setName(PARAM_TOPIC_B);
                topicBReq.partitions().add(new ListOffsetsPartition()
                        .setPartitionIndex(0)
                        .setTimestamp(-1));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.LIST_OFFSETS, apiVersion, "test-client", request));
                var body = (ListOffsetsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(ListOffsetsTopicResponse::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);
                for (var topic : body.topics()) {
                    var partition = topic.partitions().get(0);
                    assertThat(partition.errorCode())
                            .as("v%d partition error for %s", apiVersion, topic.name())
                            .isEqualTo(Errors.NONE.code());
                    assertThat(partition.offset())
                            .as("v%d latest offset for %s", apiVersion, topic.name())
                            .isGreaterThanOrEqualTo(1);
                }
            }
        }

        var listOffsetsToA = routingCaptor.requestsToRoute("route-a", ApiKeys.LIST_OFFSETS);
        var listOffsetsToB = routingCaptor.requestsToRoute("route-b", ApiKeys.LIST_OFFSETS);
        assertThat(listOffsetsToA).as("v%d: LIST_OFFSETS should route to route-a", apiVersion).isNotEmpty();
        assertThat(listOffsetsToB).as("v%d: LIST_OFFSETS should route to route-b", apiVersion).isNotEmpty();

        for (var event : listOffsetsToA) {
            var body = (ListOffsetsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : listOffsetsToB) {
            var body = (ListOffsetsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
