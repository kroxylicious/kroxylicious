/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetCommitDecomposerTest {

    private final OffsetCommitDecomposer decomposer = OffsetCommitDecomposer.INSTANCE;

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
        var request = offsetCommitRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = offsetCommitRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = offsetCommitRequest("a.orders", "b.logs");
        request.setGroupId("test-group");
        request.setGenerationIdOrMemberEpoch(5);
        request.setMemberId("member-1");
        request.setGroupInstanceId("instance-1");
        request.setRetentionTimeMs(60000);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.groupId()).isEqualTo("test-group");
            assertThat(sub.generationIdOrMemberEpoch()).isEqualTo(5);
            assertThat(sub.memberId()).isEqualTo("member-1");
            assertThat(sub.groupInstanceId()).isEqualTo("instance-1");
            assertThat(sub.retentionTimeMs()).isEqualTo(60000);
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = offsetCommitRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = offsetCommitRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionData() {
        var request = new OffsetCommitRequestData().setGroupId("g");
        var topic = new OffsetCommitRequestTopic().setName("a.orders");
        topic.partitions().add(new OffsetCommitRequestPartition()
                .setPartitionIndex(0).setCommittedOffset(100));
        topic.partitions().add(new OffsetCommitRequestPartition()
                .setPartitionIndex(1).setCommittedOffset(200));
        request.topics().add(topic);

        var parts = decomposer.decompose(request, table);

        var decomposed = parts.get("route-a").topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(decomposed.partitions()).extracting("partitionIndex")
                .containsExactly(0, 1);
        assertThat(decomposed.partitions()).extracting("committedOffset")
                .containsExactly(100L, 200L);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = offsetCommitRequest("a.orders", "b.logs");

        var respA = new OffsetCommitResponseData();
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new OffsetCommitResponseData();
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = offsetCommitRequest("a.orders", "b.logs");

        var respA = new OffsetCommitResponseData().setThrottleTimeMs(100);
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new OffsetCommitResponseData().setThrottleTimeMs(300);
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldPreservePartitionErrorCodes() {
        var request = offsetCommitRequest("a.orders");

        var resp = new OffsetCommitResponseData();
        resp.topics().add(topicResponse("a.orders", 0, Errors.NOT_COORDINATOR));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var partition = merged.topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow()
                .partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.NOT_COORDINATOR.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = new OffsetCommitRequestData().setGroupId("g");
        var topic = new OffsetCommitRequestTopic().setName("unknown.topic");
        topic.partitions().add(new OffsetCommitRequestPartition()
                .setPartitionIndex(0).setCommittedOffset(10));
        topic.partitions().add(new OffsetCommitRequestPartition()
                .setPartitionIndex(1).setCommittedOffset(20));
        request.topics().add(topic);

        var error = OffsetCommitDecomposer.errorResponseForUnroutableTopics(request, table);

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
        var request = offsetCommitRequest("a.orders", "b.logs");

        var error = OffsetCommitDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).isEmpty();
    }

    // --- helpers ---

    private static OffsetCommitRequestData offsetCommitRequest(String... topicNames) {
        var request = new OffsetCommitRequestData().setGroupId("test-group");
        for (var name : topicNames) {
            var topic = new OffsetCommitRequestTopic().setName(name);
            topic.partitions().add(new OffsetCommitRequestPartition()
                    .setPartitionIndex(0).setCommittedOffset(0));
            request.topics().add(topic);
        }
        return request;
    }

    private static OffsetCommitResponseTopic topicResponse(String name,
                                                           int partition,
                                                           Errors error) {
        var tr = new OffsetCommitResponseTopic().setName(name);
        tr.partitions().add(
                new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        return tr;
    }
}
