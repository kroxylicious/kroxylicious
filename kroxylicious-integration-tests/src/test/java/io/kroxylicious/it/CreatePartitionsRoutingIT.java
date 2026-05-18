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
import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
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
 * Integration tests for CREATE_PARTITIONS routing through the topic-partition router.
 */
class CreatePartitionsRoutingIT extends TopicPartitionRoutingBaseIT {

    @Test
    void shouldCreatePartitionsOnBothRoutes() throws Exception {
        String topicA = "a.partitions-test";
        String topicB = "b.partitions-test";
        createTopicOnCluster(topicA, 1, clusterA);
        createTopicOnCluster(topicB, 1, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new CreatePartitionsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(false);
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicA)
                        .setCount(3)
                        .setAssignments(null));
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicB)
                        .setCount(3)
                        .setAssignments(null));

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_PARTITIONS, (short) 3, "test-client", request));
                var body = (CreatePartitionsResponseData) response.payload().message();

                assertThat(body.results()).extracting(CreatePartitionsTopicResult::name)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.results()) {
                    assertThat(topic.errorCode())
                            .as("error for %s", topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        // Verify partition counts on the correct backend clusters
        try (var admin = AdminClient.create(clusterA.getKafkaClientConfiguration())) {
            var desc = admin.describeTopics(List.of(topicA)).topicNameValues()
                    .get(topicA).get(10, TimeUnit.SECONDS);
            assertThat(desc.partitions()).hasSize(3);
        }
        try (var admin = AdminClient.create(clusterB.getKafkaClientConfiguration())) {
            var desc = admin.describeTopics(List.of(topicB)).topicNameValues()
                    .get(topicB).get(10, TimeUnit.SECONDS);
            assertThat(desc.partitions()).hasSize(3);
        }

        // Verify routing
        var createPartToA = routingCaptor.requestsToRoute("route-a", ApiKeys.CREATE_PARTITIONS);
        var createPartToB = routingCaptor.requestsToRoute("route-b", ApiKeys.CREATE_PARTITIONS);
        assertThat(createPartToA).as("CREATE_PARTITIONS should be routed to route-a").isNotEmpty();
        assertThat(createPartToB).as("CREATE_PARTITIONS should be routed to route-b").isNotEmpty();

        for (var event : createPartToA) {
            var body = (CreatePartitionsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-a should only receive a.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : createPartToB) {
            var body = (CreatePartitionsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("route-b should only receive b.* topics")
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }

    @Test
    void shouldRejectCreatePartitionsWithExplicitAssignments() throws Exception {
        String topicWithAssign = "a.partitions-assigned";
        String topicAutoAssign = "a.partitions-auto";
        createTopicOnCluster(topicWithAssign, 1, clusterA);
        createTopicOnCluster(topicAutoAssign, 1, clusterA);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new CreatePartitionsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(true);
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicWithAssign)
                        .setCount(3)
                        .setAssignments(List.of(
                                new CreatePartitionsAssignment().setBrokerIds(List.of(0)),
                                new CreatePartitionsAssignment().setBrokerIds(List.of(0)))));
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicAutoAssign)
                        .setCount(3)
                        .setAssignments(null));

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_PARTITIONS, (short) 3, "test-client", request));
                var body = (CreatePartitionsResponseData) response.payload().message();

                var assigned = body.results().stream()
                        .filter(t -> t.name().equals(topicWithAssign))
                        .findFirst().orElseThrow();
                assertThat(assigned.errorCode())
                        .as("topic with explicit assignments should be rejected")
                        .isEqualTo(Errors.INVALID_REPLICA_ASSIGNMENT.code());

                var auto = body.results().stream()
                        .filter(t -> t.name().equals(topicAutoAssign))
                        .findFirst().orElseThrow();
                assertThat(auto.errorCode())
                        .as("topic without assignments should succeed")
                        .isEqualTo(Errors.NONE.code());
            }
        }
    }

    // --- Version sweep ---

    static List<Arguments> createPartitionsVersions() {
        var result = new ArrayList<Arguments>();
        for (short v = 0; v <= ApiKeys.CREATE_PARTITIONS.latestVersion(); v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("createPartitionsVersions")
    void createPartitionsAcrossVersions(short apiVersion) throws Exception {
        String topicA = "a.partitions-v" + apiVersion;
        String topicB = "b.partitions-v" + apiVersion;
        createTopicOnCluster(topicA, 1, clusterA);
        createTopicOnCluster(topicB, 1, clusterB);
        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new CreatePartitionsRequestData()
                        .setTimeoutMs(30000)
                        .setValidateOnly(true);
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicA)
                        .setCount(3)
                        .setAssignments(null));
                request.topics().add(new CreatePartitionsTopic()
                        .setName(topicB)
                        .setCount(3)
                        .setAssignments(null));

                var response = client.getSync(
                        new Request(ApiKeys.CREATE_PARTITIONS, apiVersion, "test-client", request));
                var body = (CreatePartitionsResponseData) response.payload().message();

                assertThat(body.results()).extracting(CreatePartitionsTopicResult::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(topicA, topicB);
                for (var topic : body.results()) {
                    assertThat(topic.errorCode())
                            .as("v%d error for %s", apiVersion, topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        var createPartToA = routingCaptor.requestsToRoute("route-a", ApiKeys.CREATE_PARTITIONS);
        var createPartToB = routingCaptor.requestsToRoute("route-b", ApiKeys.CREATE_PARTITIONS);
        assertThat(createPartToA).as("v%d: CREATE_PARTITIONS should route to route-a", apiVersion).isNotEmpty();
        assertThat(createPartToB).as("v%d: CREATE_PARTITIONS should route to route-b", apiVersion).isNotEmpty();

        for (var event : createPartToA) {
            var body = (CreatePartitionsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : createPartToB) {
            var body = (CreatePartitionsRequestData) event.body();
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
