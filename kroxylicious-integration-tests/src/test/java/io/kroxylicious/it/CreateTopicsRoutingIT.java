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
import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
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
 * Integration tests for CREATE_TOPICS routing through the topic-partition router.
 */
class CreateTopicsRoutingIT extends TopicPartitionRoutingBaseIT {

    @Test
    void shouldCreateTopicsOnBothRoutes() throws Exception {
        String topicA = "a.create-test";
        String topicB = "b.create-test";
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new CreateTopicsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(false);
                request.topics().add(new CreatableTopic()
                        .setName(topicA)
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1));
                request.topics().add(new CreatableTopic()
                        .setName(topicB)
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1));

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_TOPICS, (short) 5, "test-client", request));
                var body = (CreateTopicsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(CreatableTopicResult::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.topics()) {
                    assertThat(topic.errorCode())
                            .as("error for %s", topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        // Verify topics were created on the correct backend clusters
        try (var admin = AdminClient.create(clusterA.getKafkaClientConfiguration())) {
            var names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).contains(topicA);
            assertThat(names).doesNotContain(topicB);
        }
        try (var admin = AdminClient.create(clusterB.getKafkaClientConfiguration())) {
            var names = admin.listTopics().names().get(10, TimeUnit.SECONDS);
            assertThat(names).contains(topicB);
            assertThat(names).doesNotContain(topicA);
        }

        // Verify routing
        var createToA = routingCaptor.requestsToRoute("route-a", ApiKeys.CREATE_TOPICS);
        var createToB = routingCaptor.requestsToRoute("route-b", ApiKeys.CREATE_TOPICS);
        assertThat(createToA).as("CREATE_TOPICS should be routed to route-a").isNotEmpty();
        assertThat(createToB).as("CREATE_TOPICS should be routed to route-b").isNotEmpty();

        for (var event : createToA) {
            var body = (CreateTopicsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : createToB) {
            var body = (CreateTopicsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    @Test
    void shouldRejectCreateTopicsWithExplicitAssignments() throws Exception {
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var topicWithAssignments = new CreatableTopic()
                        .setName("a.assigned-topic")
                        .setNumPartitions(-1)
                        .setReplicationFactor((short) -1);
                topicWithAssignments.assignments().add(new CreatableReplicaAssignment()
                        .setPartitionIndex(0)
                        .setBrokerIds(List.of(0)));
                var topicWithoutAssignments = new CreatableTopic()
                        .setName("a.auto-topic")
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1);

                var request = new CreateTopicsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(true);
                request.topics().add(topicWithAssignments);
                request.topics().add(topicWithoutAssignments);

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_TOPICS, (short) 5, "test-client", request));
                var body = (CreateTopicsResponseData) response.payload().message();

                var assigned = body.topics().stream()
                        .filter(t -> t.name().equals("a.assigned-topic"))
                        .findFirst().orElseThrow();
                assertThat(assigned.errorCode())
                        .as("topic with explicit assignments should be rejected")
                        .isEqualTo(Errors.INVALID_REPLICA_ASSIGNMENT.code());

                var auto = body.topics().stream()
                        .filter(t -> t.name().equals("a.auto-topic"))
                        .findFirst().orElseThrow();
                assertThat(auto.errorCode())
                        .as("topic without assignments should succeed")
                        .isEqualTo(Errors.NONE.code());
            }
        }
    }

    // --- Version sweep ---

    static List<Arguments> createTopicsVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 2; v <= ApiKeys.CREATE_TOPICS.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("createTopicsVersions")
    void createTopicsAcrossVersions(short apiVersion) throws Exception {
        String topicA = "a.create-v" + apiVersion;
        String topicB = "b.create-v" + apiVersion;
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new CreateTopicsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(true);
                request.topics().add(new CreatableTopic()
                        .setName(topicA)
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1));
                request.topics().add(new CreatableTopic()
                        .setName(topicB)
                        .setNumPartitions(1)
                        .setReplicationFactor((short) 1));

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_TOPICS, apiVersion, "test-client", request));
                var body = (CreateTopicsResponseData) response.payload().message();

                assertThat(body.topics()).extracting(CreatableTopicResult::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.topics()) {
                    assertThat(topic.errorCode())
                            .as("v%d error for %s", apiVersion, topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        var createToA = routingCaptor.requestsToRoute("route-a", ApiKeys.CREATE_TOPICS);
        var createToB = routingCaptor.requestsToRoute("route-b", ApiKeys.CREATE_TOPICS);
        assertThat(createToA).as("v%d: CREATE_TOPICS should route to route-a", apiVersion).isNotEmpty();
        assertThat(createToB).as("v%d: CREATE_TOPICS should route to route-b", apiVersion).isNotEmpty();

        for (var event : createToA) {
            var body = (CreateTopicsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : createToB) {
            var body = (CreateTopicsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
