/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.DeleteRecordsRequestData;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsPartition;
import org.apache.kafka.common.message.DeleteRecordsRequestData.DeleteRecordsTopic;
import org.apache.kafka.common.message.DeleteRecordsResponseData;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsPartitionResult;
import org.apache.kafka.common.message.DeleteRecordsResponseData.DeleteRecordsTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteRecordsDecomposerTest {

    private final DeleteRecordsDecomposer decomposer = DeleteRecordsDecomposer.INSTANCE;

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
        var request = deleteRecordsRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = deleteRecordsRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = deleteRecordsRequest("a.orders", "b.logs");
        request.setTimeoutMs(15000);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.timeoutMs()).isEqualTo(15000);
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = deleteRecordsRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = deleteRecordsRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionData() {
        var request = new DeleteRecordsRequestData();
        var topic = new DeleteRecordsTopic().setName("a.orders");
        topic.partitions().add(new DeleteRecordsPartition().setPartitionIndex(0).setOffset(100));
        topic.partitions().add(new DeleteRecordsPartition().setPartitionIndex(1).setOffset(200));
        request.topics().add(topic);

        var parts = decomposer.decompose(request, table);

        var decomposed = parts.get("route-a").topics().iterator().next();
        assertThat(decomposed.partitions()).extracting("partitionIndex")
                .containsExactly(0, 1);
        assertThat(decomposed.partitions()).extracting("offset")
                .containsExactly(100L, 200L);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = deleteRecordsRequest("a.orders", "b.logs");

        var respA = new DeleteRecordsResponseData();
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new DeleteRecordsResponseData();
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = deleteRecordsRequest("a.orders", "b.logs");

        var respA = new DeleteRecordsResponseData().setThrottleTimeMs(80);
        respA.topics().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new DeleteRecordsResponseData().setThrottleTimeMs(250);
        respB.topics().add(topicResponse("b.logs", 0, Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(250);
    }

    @Test
    void shouldPreservePartitionErrorCodes() {
        var request = deleteRecordsRequest("a.orders");

        var resp = new DeleteRecordsResponseData();
        resp.topics().add(topicResponse("a.orders", 0, Errors.OFFSET_OUT_OF_RANGE));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var topic = merged.topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        var partition = topic.partitions().iterator().next();
        assertThat(partition.errorCode()).isEqualTo(Errors.OFFSET_OUT_OF_RANGE.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = new DeleteRecordsRequestData();
        var topic = new DeleteRecordsTopic().setName("unknown.topic");
        topic.partitions().add(new DeleteRecordsPartition().setPartitionIndex(0));
        topic.partitions().add(new DeleteRecordsPartition().setPartitionIndex(1));
        request.topics().add(topic);

        var error = DeleteRecordsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).hasSize(1);
        var topicResp = error.topics().iterator().next();
        assertThat(topicResp.name()).isEqualTo("unknown.topic");
        assertThat(topicResp.partitions()).hasSize(2);
        assertThat(topicResp.partitions()).allSatisfy(
                pr -> assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = deleteRecordsRequest("a.orders", "b.logs");

        var error = DeleteRecordsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).isEmpty();
    }

    // --- helpers ---

    private static DeleteRecordsRequestData deleteRecordsRequest(String... topicNames) {
        var request = new DeleteRecordsRequestData();
        for (var name : topicNames) {
            var topic = new DeleteRecordsTopic().setName(name);
            topic.partitions().add(new DeleteRecordsPartition().setPartitionIndex(0).setOffset(-1));
            request.topics().add(topic);
        }
        return request;
    }

    private static DeleteRecordsTopicResult topicResponse(String name,
                                                          int partition,
                                                          Errors error) {
        var tr = new DeleteRecordsTopicResult().setName(name);
        tr.partitions().add(
                new DeleteRecordsPartitionResult()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        return tr;
    }
}
