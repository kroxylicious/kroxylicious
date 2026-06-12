/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchRequestData.ForgottenTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class FetchDecomposerTest {

    private final FetchDecomposer decomposer = FetchDecomposer.INSTANCE;

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
        var request = fetchRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("topic")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("topic")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = fetchRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = fetchRequest("a.orders", "b.logs");
        request.setMaxWaitMs(500);
        request.setMinBytes(1);
        request.setMaxBytes(1048576);
        request.setIsolationLevel((byte) 1);
        request.setRackId("rack-1");

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.maxWaitMs()).isEqualTo(500);
            assertThat(sub.minBytes()).isEqualTo(1);
            assertThat(sub.maxBytes()).isEqualTo(1048576);
            assertThat(sub.isolationLevel()).isEqualTo((byte) 1);
            assertThat(sub.rackId()).isEqualTo("rack-1");
        }
    }

    @Test
    void shouldNotCopySessionFields() {
        var request = fetchRequest("a.orders", "b.logs");
        request.setSessionId(42);
        request.setSessionEpoch(3);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.sessionId())
                    .as("sub-requests should not inherit original session ID")
                    .isNotEqualTo(42);
            assertThat(sub.sessionEpoch())
                    .as("sub-requests should not inherit original session epoch")
                    .isNotEqualTo(3);
        }
    }

    @Test
    void shouldNotCopyForgottenTopicsData() {
        var request = fetchRequest("a.orders");
        request.forgottenTopicsData().add(
                new ForgottenTopic().setTopic("old.topic").setPartitions(java.util.List.of(0)));

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.forgottenTopicsData())
                    .as("sub-requests should not include forgotten topics from original request")
                    .isEmpty();
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = fetchRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("topic")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = fetchRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionData() {
        var request = new FetchRequestData();
        var topic = new FetchTopic().setTopic("a.orders");
        topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(100));
        topic.partitions().add(new FetchPartition().setPartition(1).setFetchOffset(200));
        request.topics().add(topic);

        var parts = decomposer.decompose(request, table);

        var decomposed = parts.get("route-a").topics().stream()
                .filter(t -> t.topic().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(decomposed.partitions()).extracting("partition")
                .containsExactly(0, 1);
        assertThat(decomposed.partitions()).extracting("fetchOffset")
                .containsExactly(100L, 200L);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = fetchRequest("a.orders", "b.logs");

        var respA = new FetchResponseData();
        respA.responses().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new FetchResponseData();
        respB.responses().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.responses()).extracting("topic")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = fetchRequest("a.orders", "b.logs");

        var respA = new FetchResponseData().setThrottleTimeMs(100);
        respA.responses().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new FetchResponseData().setThrottleTimeMs(300);
        respB.responses().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldNotSetSessionFieldsInResponse() {
        var request = fetchRequest("a.orders");

        var resp = new FetchResponseData().setSessionId(42);
        resp.responses().add(topicResponse("a.orders", 0, Errors.NONE));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        assertThat(merged.sessionId())
                .as("recompose should not override session ID — FetchSessionManager handles that")
                .isEqualTo(0);
    }

    @Test
    void shouldPreservePartitionErrorCodes() {
        var request = fetchRequest("a.orders");

        var resp = new FetchResponseData();
        resp.responses().add(topicResponse("a.orders", 0, Errors.NOT_LEADER_OR_FOLLOWER));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var partition = merged.responses().stream()
                .filter(t -> t.topic().equals("a.orders"))
                .findFirst().orElseThrow()
                .partitions().get(0);
        assertThat(partition.errorCode()).isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = new FetchRequestData();
        var topic = new FetchTopic().setTopic("unknown.topic");
        topic.partitions().add(new FetchPartition().setPartition(0));
        topic.partitions().add(new FetchPartition().setPartition(1));
        request.topics().add(topic);

        var error = FetchDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).hasSize(1);
        var topicResp = error.responses().stream()
                .filter(t -> t.topic().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(topicResp.partitions()).hasSize(2);
        assertThat(topicResp.partitions()).allSatisfy(
                pr -> assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = fetchRequest("a.orders", "b.logs");

        var error = FetchDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).isEmpty();
    }

    // --- helpers ---

    private static FetchRequestData fetchRequest(String... topicNames) {
        var request = new FetchRequestData();
        for (var name : topicNames) {
            var topic = new FetchTopic().setTopic(name);
            topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
            request.topics().add(topic);
        }
        return request;
    }

    private static FetchableTopicResponse topicResponse(String name,
                                                        int partition,
                                                        Errors error) {
        var tr = new FetchableTopicResponse().setTopic(name);
        tr.partitions().add(
                new PartitionData()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        return tr;
    }
}
