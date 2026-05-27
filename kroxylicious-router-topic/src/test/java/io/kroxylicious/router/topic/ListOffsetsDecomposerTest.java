/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class ListOffsetsDecomposerTest {

    private final ListOffsetsDecomposer decomposer = ListOffsetsDecomposer.INSTANCE;

    private final TopicRoutingTable table = new TopicRoutingTable() {
        @Override
        @Nullable
        public String routeForTopic(String topicName) {
            if (topicName.startsWith("a.")) {
                return "route-a";
            }
            if (topicName.startsWith("b.")) {
                return "route-b";
            }
            return null;
        }

        @Override
        public Set<String> allRoutes() {
            return Set.of("route-a", "route-b");
        }
    };

    // --- decompose ---

    @Test
    void shouldDecomposeByTopicRoute() {
        var request = listOffsetsRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = listOffsetsRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = listOffsetsRequest("a.orders", "b.logs");
        request.setReplicaId(-1);
        request.setIsolationLevel((byte) 1);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.replicaId()).isEqualTo(-1);
            assertThat(sub.isolationLevel()).isEqualTo((byte) 1);
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = listOffsetsRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = listOffsetsRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionData() {
        var request = new ListOffsetsRequestData();
        var topic = new ListOffsetsTopic().setName("a.orders");
        topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
        topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(1).setTimestamp(-2));
        request.topics().add(topic);

        var parts = decomposer.decompose(request, table);

        var decomposed = parts.get("route-a").topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(decomposed.partitions()).extracting("partitionIndex")
                .containsExactly(0, 1);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = listOffsetsRequest("a.orders", "b.logs");

        var respA = new ListOffsetsResponseData();
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new ListOffsetsResponseData();
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = listOffsetsRequest("a.orders", "b.logs");

        var respA = new ListOffsetsResponseData().setThrottleTimeMs(100);
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new ListOffsetsResponseData().setThrottleTimeMs(300);
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldPreservePartitionErrorCodes() {
        var request = listOffsetsRequest("a.orders");

        var resp = new ListOffsetsResponseData();
        resp.topics().add(topicResponse("a.orders", 0, Errors.NOT_LEADER_OR_FOLLOWER));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var partition = merged.topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow()
                .partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = new ListOffsetsRequestData();
        var topic = new ListOffsetsTopic().setName("unknown.topic");
        topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(0));
        topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(1));
        request.topics().add(topic);

        var error = ListOffsetsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).hasSize(1);
        var topicResp = error.topics().stream()
                .filter(t -> t.name().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(topicResp.partitions()).hasSize(2);
        assertThat(topicResp.partitions()).allSatisfy(
                pr -> assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = listOffsetsRequest("a.orders", "b.logs");

        var error = ListOffsetsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).isEmpty();
    }

    // --- helpers ---

    private static ListOffsetsRequestData listOffsetsRequest(String... topicNames) {
        var request = new ListOffsetsRequestData();
        for (var name : topicNames) {
            var topic = new ListOffsetsTopic().setName(name);
            topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
            request.topics().add(topic);
        }
        return request;
    }

    private static ListOffsetsTopicResponse topicResponse(String name,
                                                          int partition,
                                                          Errors error) {
        var tr = new ListOffsetsTopicResponse().setName(name);
        tr.partitions().add(
                new ListOffsetsPartitionResponse()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        return tr;
    }
}
