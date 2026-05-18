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
import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
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
 * Integration tests for DELETE_RECORDS routing through the topic-partition router.
 */
class DeleteRecordsRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.delrec-param";
    private static final String PARAM_TOPIC_B = "b.delrec-param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

    @Test
    void shouldDeleteRecordsOnBothRoutes() throws Exception {
        String topicA = "a.delrecords";
        String topicB = "b.delrecords";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            // Produce records so there's something to delete
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

                var request = new DeleteRecordsRequestData()
                        .setTimeoutMs(30000);
                var topicAReq = new DeleteRecordsTopic().setName(topicA);
                topicAReq.partitions().add(new DeleteRecordsPartition()
                        .setPartitionIndex(0)
                        .setOffset(-1));
                var topicBReq = new DeleteRecordsTopic().setName(topicB);
                topicBReq.partitions().add(new DeleteRecordsPartition()
                        .setPartitionIndex(0)
                        .setOffset(-1));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.DELETE_RECORDS, (short) 2, "test-client", request));
                var body = (DeleteRecordsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(DeleteRecordsTopicResult::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.topics()) {
                    for (var partition : topic.partitions()) {
                        assertThat(partition.errorCode())
                                .as("partition %d error for %s", partition.partitionIndex(), topic.name())
                                .isEqualTo(Errors.NONE.code());
                        assertThat(partition.lowWatermark())
                                .as("low watermark for %s-%d", topic.name(), partition.partitionIndex())
                                .isGreaterThanOrEqualTo(0);
                    }
                }
            }
        }

        // Verify routing
        var deleteRecToA = routingCaptor.requestsToRoute("route-a", ApiKeys.DELETE_RECORDS);
        var deleteRecToB = routingCaptor.requestsToRoute("route-b", ApiKeys.DELETE_RECORDS);
        assertThat(deleteRecToA).as("DELETE_RECORDS should be routed to route-a").isNotEmpty();
        assertThat(deleteRecToB).as("DELETE_RECORDS should be routed to route-b").isNotEmpty();

        for (var event : deleteRecToA) {
            var body = (DeleteRecordsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : deleteRecToB) {
            var body = (DeleteRecordsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    // --- Version sweep ---

    static List<Arguments> deleteRecordsVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 0; v <= ApiKeys.DELETE_RECORDS.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("deleteRecordsVersions")
    void deleteRecordsAcrossVersions(short apiVersion) throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            // Produce records to delete
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

                var request = new DeleteRecordsRequestData()
                        .setTimeoutMs(30000);
                var topicAReq = new DeleteRecordsTopic().setName(PARAM_TOPIC_A);
                topicAReq.partitions().add(new DeleteRecordsPartition()
                        .setPartitionIndex(0)
                        .setOffset(-1));
                var topicBReq = new DeleteRecordsTopic().setName(PARAM_TOPIC_B);
                topicBReq.partitions().add(new DeleteRecordsPartition()
                        .setPartitionIndex(0)
                        .setOffset(-1));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.DELETE_RECORDS, apiVersion, "test-client", request));
                var body = (DeleteRecordsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(DeleteRecordsTopicResult::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);
                for (var topic : body.topics()) {
                    for (var partition : topic.partitions()) {
                        assertThat(partition.errorCode())
                                .as("v%d partition error for %s", apiVersion, topic.name())
                                .isEqualTo(Errors.NONE.code());
                    }
                }
            }
        }

        var deleteRecToA = routingCaptor.requestsToRoute("route-a", ApiKeys.DELETE_RECORDS);
        var deleteRecToB = routingCaptor.requestsToRoute("route-b", ApiKeys.DELETE_RECORDS);
        assertThat(deleteRecToA).as("v%d: DELETE_RECORDS should route to route-a", apiVersion).isNotEmpty();
        assertThat(deleteRecToB).as("v%d: DELETE_RECORDS should route to route-b", apiVersion).isNotEmpty();

        for (var event : deleteRecToA) {
            var body = (DeleteRecordsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : deleteRecToB) {
            var body = (DeleteRecordsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
