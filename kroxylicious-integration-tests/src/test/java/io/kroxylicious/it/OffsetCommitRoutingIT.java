/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.it;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
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
 * Integration tests for OFFSET_COMMIT router through the topic-partition router.
 */
class OffsetCommitRoutingIT extends TopicPartitionRoutingBaseIT {

    private static final String PARAM_TOPIC_A = "a.param";
    private static final String PARAM_TOPIC_B = "b.param";
    private static final String GROUP_ID = "test-simple-group";

    @BeforeAll
    static void createParameterisedTestTopics() throws Exception {
        createTopicOnCluster(PARAM_TOPIC_A, 1, clusterA);
        createTopicOnCluster(PARAM_TOPIC_B, 1, clusterB);
    }

    @Test
    void shouldCommitOffsetsForTopicsOnBothRoutes() throws Exception {
        String topicA = "a.commitoffsets";
        String topicB = "b.commitoffsets";
        String groupId = "test-simple-group";
        createTopic(topicA, clusterA);
        createTopic(topicB, clusterB);

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

    // --- Version sweep ---

    static List<Arguments> offsetCommitVersions() {
        var result = new ArrayList<Arguments>();
        // OffsetCommitRequestTopic can't be serialised at v0–v1 (schema structure changed at v2)
        for (short v = 2; v <= 9; v++) {
            result.add(Arguments.of(v));
        }
        return result;
    }

    @ParameterizedTest
    @MethodSource("offsetCommitVersions")
    void offsetCommitAcrossVersions(short apiVersion) throws Exception {
        warmUpGroupCoordinator(clusterA, PARAM_TOPIC_A, GROUP_ID);
        warmUpGroupCoordinator(clusterB, PARAM_TOPIC_B, GROUP_ID);

        var config = topicRouterConfig();

        try (var tester = kroxyliciousTester(config)) {
            try (var client = tester.simpleTestClient()) {
                negotiateApiVersions(client);

                var request = new OffsetCommitRequestData()
                        .setGroupId(GROUP_ID)
                        .setMemberId("")
                        .setGenerationIdOrMemberEpoch(-1);
                var topicAReq = new OffsetCommitRequestTopic().setName(PARAM_TOPIC_A);
                topicAReq.partitions().add(new OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(42));
                var topicBReq = new OffsetCommitRequestTopic().setName(PARAM_TOPIC_B);
                topicBReq.partitions().add(new OffsetCommitRequestPartition()
                        .setPartitionIndex(0)
                        .setCommittedOffset(99));
                request.topics().add(topicAReq);
                request.topics().add(topicBReq);

                var response = client.getSync(
                        new Request(ApiKeys.OFFSET_COMMIT, apiVersion, "test-client", request));
                var body = (OffsetCommitResponseData) response.payload().message();

                assertThat(body.topics()).extracting(OffsetCommitResponseTopic::name)
                        .as("v%d response should contain both topics", apiVersion)
                        .containsExactlyInAnyOrder(PARAM_TOPIC_A, PARAM_TOPIC_B);
                for (var topic : body.topics()) {
                    assertThat(topic.partitions().get(0).errorCode())
                            .as("v%d offset commit error for %s", apiVersion, topic.name())
                            .isEqualTo(Errors.NONE.code());
                }
            }
        }

        var commitsToA = routingCaptor.requestsToRoute("route-a", ApiKeys.OFFSET_COMMIT);
        var commitsToB = routingCaptor.requestsToRoute("route-b", ApiKeys.OFFSET_COMMIT);
        assertThat(commitsToA).as("v%d: OFFSET_COMMIT should route to route-a", apiVersion).isNotEmpty();
        assertThat(commitsToB).as("v%d: OFFSET_COMMIT should route to route-b", apiVersion).isNotEmpty();

        for (var event : commitsToA) {
            var body = (OffsetCommitRequestData) event.body();
            assertThat(body.groupId()).as("v%d: groupId preserved on route-a", apiVersion).isEqualTo(GROUP_ID);
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-a should only receive a.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("a."));
        }
        for (var event : commitsToB) {
            var body = (OffsetCommitRequestData) event.body();
            assertThat(body.groupId()).as("v%d: groupId preserved on route-b", apiVersion).isEqualTo(GROUP_ID);
            assertThat(body.topics()).extracting("name")
                    .as("v%d: route-b should only receive b.* topics", apiVersion)
                    .allSatisfy(name -> assertThat((String) name).startsWith("b."));
        }
    }
}
