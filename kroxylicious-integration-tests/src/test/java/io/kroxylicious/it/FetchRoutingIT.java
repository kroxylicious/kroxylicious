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
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.integration.Request;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for FETCH routing through the topic-partition router.
 */
class FetchRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

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

    // --- Version sweep ---

    static List<Arguments> fetchVersions() {
        var result = new ArrayList<Arguments>();
        // FetchTopic can't be serialised at v0–v3 (schema structure changed at v4)
        for (short v = 4; v <= 12; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("fetchVersions")
    void fetchAcrossVersions(short apiVersion) throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            // Produce records via real client first
            try (var producer = tester.producer(Map.of(
                    "enable.idempotence", false,
                    "retries", 0,
                    "batch.size", 0,
                    "linger.ms", 0))) {
                producer.send(new ProducerRecord<>(PARAM_TOPIC_A, "key-a", "val-a"))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(PARAM_TOPIC_B, "key-b", "val-b"))
                        .get(10, TimeUnit.SECONDS);
            }

            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new FetchRequestData()
                        .setMaxWaitMs(5000)
                        .setMinBytes(1)
                        .setMaxBytes(1024 * 1024)
                        .setReplicaId(-1)
                        .setSessionId(0)
                        .setSessionEpoch(-1);

                var topicA = new FetchRequestData.FetchTopic()
                        .setTopic(PARAM_TOPIC_A);
                topicA.partitions().add(new FetchRequestData.FetchPartition()
                        .setPartition(0)
                        .setFetchOffset(0)
                        .setPartitionMaxBytes(1024 * 1024));
                request.topics().add(topicA);

                var topicB = new FetchRequestData.FetchTopic()
                        .setTopic(PARAM_TOPIC_B);
                topicB.partitions().add(new FetchRequestData.FetchPartition()
                        .setPartition(0)
                        .setFetchOffset(0)
                        .setPartitionMaxBytes(1024 * 1024));
                request.topics().add(topicB);

                var response = client.getSync(
                        new Request(ApiKeys.FETCH, apiVersion, "test-client", request));
                var body = (FetchResponseData) response.payload().message();

                assertThat(body.responses())
                        .as("v%d FETCH response should contain topics", apiVersion)
                        .isNotEmpty();

                assertThat(body.responses()).extracting(FetchResponseData.FetchableTopicResponse::topic)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);

                for (var topicResp : body.responses()) {
                    for (var partResp : topicResp.partitions()) {
                        assertThat(partResp.errorCode())
                                .as("v%d partition error for %s", apiVersion, topicResp.topic())
                                .isEqualTo(Errors.NONE.code());
                    }
                }
            }
        }

        var fetchesToA = routingCaptor.requestsToRoute("route-a", ApiKeys.FETCH);
        var fetchesToB = routingCaptor.requestsToRoute("route-b", ApiKeys.FETCH);
        assertThat(fetchesToA).as("v%d: FETCH should route to route-a", apiVersion).isNotEmpty();
        assertThat(fetchesToB).as("v%d: FETCH should route to route-b", apiVersion).isNotEmpty();

        for (var event : fetchesToA) {
            var body = (FetchRequestData) event.body();
            assertThat(body.sessionId()).as("v%d: sessionId forced to 0 on route-a", apiVersion).isEqualTo(0);
            assertThat(body.sessionEpoch()).as("v%d: sessionEpoch forced to -1 on route-a", apiVersion).isEqualTo(-1);
            assertThat(body.topics()).extracting("topic")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : fetchesToB) {
            var body = (FetchRequestData) event.body();
            assertThat(body.sessionId()).as("v%d: sessionId forced to 0 on route-b", apiVersion).isEqualTo(0);
            assertThat(body.sessionEpoch()).as("v%d: sessionEpoch forced to -1 on route-b", apiVersion).isEqualTo(-1);
            assertThat(body.topics()).extracting("topic")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
