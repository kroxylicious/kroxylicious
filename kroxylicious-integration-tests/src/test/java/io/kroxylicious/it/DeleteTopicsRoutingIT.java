/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.testing.integration.Request;

import static io.kroxylicious.testing.integration.tester.KroxyliciousTesters.kroxyliciousTester;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for DELETE_TOPICS routing through the topic-partition router.
 * DELETE_TOPICS is capped at v5 to force name-based addressing.
 */
class DeleteTopicsRoutingIT extends TopicPartitionRoutingBaseIT {

    @Test
    void shouldDeleteTopicsOnBothRoutes() throws Exception {
        String topicA = "a.delete-test";
        String topicB = "b.delete-test";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new DeleteTopicsRequestData()
                        .setTimeoutMs(30000);
                request.topicNames().add(topicA);
                request.topicNames().add(topicB);

                var response = client.getSync(
                        new Request(ApiKeys.DELETE_TOPICS, (short) 5, "test-client", request));
                var body = (DeleteTopicsResponseData) response.payload().message();

                assertThat(body.responses()).extracting(DeletableTopicResult::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.responses()) {
                    assertThat(topic.errorCode())
                            .as("error for %s", topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        // Verify topics were deleted from the correct backend clusters
        try (var admin = AdminClient.create(clusterA.getKafkaClientConfiguration())) {
            var names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).doesNotContain(topicA);
        }
        try (var admin = AdminClient.create(clusterB.getKafkaClientConfiguration())) {
            var names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).doesNotContain(topicB);
        }

        // Verify routing
        var deleteToA = routingCaptor.requestsToRoute("route-a", ApiKeys.DELETE_TOPICS);
        var deleteToB = routingCaptor.requestsToRoute("route-b", ApiKeys.DELETE_TOPICS);
        assertThat(deleteToA).as("DELETE_TOPICS should be routed to route-a").isNotEmpty();
        assertThat(deleteToB).as("DELETE_TOPICS should be routed to route-b").isNotEmpty();

        for (var event : deleteToA) {
            var body = (DeleteTopicsRequestData) event.body();
            assertThat(body.topicNames())
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat(name).startsWith("a."));
        }
        for (var event : deleteToB) {
            var body = (DeleteTopicsRequestData) event.body();
            assertThat(body.topicNames())
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat(name).startsWith("b."));
        }
    }

    // --- Version sweep ---

    static List<Arguments> deleteTopicsVersions() {
        var result = new ArrayList<Arguments>();
        // v1 is the baseline (v0 removed in Kafka 4.0), capped at v5
        for (short v = 1; v <= 5; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("deleteTopicsVersions")
    void deleteTopicsAcrossVersions(short apiVersion) throws Exception {
        String topicA = "a.delete-v" + apiVersion;
        String topicB = "b.delete-v" + apiVersion;
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new DeleteTopicsRequestData()
                        .setTimeoutMs(30000);
                request.topicNames().add(topicA);
                request.topicNames().add(topicB);

                var response = client.getSync(
                        new Request(ApiKeys.DELETE_TOPICS, apiVersion, "test-client", request));
                var body = (DeleteTopicsResponseData) response.payload().message();

                assertThat(body.responses()).extracting(DeletableTopicResult::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.responses()) {
                    assertThat(topic.errorCode())
                            .as("v%d error for %s", apiVersion, topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        var deleteToA = routingCaptor.requestsToRoute("route-a", ApiKeys.DELETE_TOPICS);
        var deleteToB = routingCaptor.requestsToRoute("route-b", ApiKeys.DELETE_TOPICS);
        assertThat(deleteToA).as("v%d: DELETE_TOPICS should route to route-a", apiVersion).isNotEmpty();
        assertThat(deleteToB).as("v%d: DELETE_TOPICS should route to route-b", apiVersion).isNotEmpty();

        for (var event : deleteToA) {
            var body = (DeleteTopicsRequestData) event.body();
            assertThat(body.topicNames())
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat(name).startsWith("a."));
        }
        for (var event : deleteToB) {
            var body = (DeleteTopicsRequestData) event.body();
            assertThat(body.topicNames())
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat(name).startsWith("b."));
        }
    }
}
