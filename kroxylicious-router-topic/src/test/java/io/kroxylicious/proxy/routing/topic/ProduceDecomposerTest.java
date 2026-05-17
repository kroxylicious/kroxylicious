/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class ProduceDecomposerTest {

    private final ProduceDecomposer decomposer = ProduceDecomposer.INSTANCE;

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
        var request = produceRequest("a.orders", "b.logs", "a.payments");

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topicData()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topicData()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = produceRequest("a.orders", "a.payments");

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topicData()).hasSize(2);
    }

    @Test
    void shouldCopyAcksAndTimeoutToSubRequests() {
        var request = produceRequest("a.orders", "b.logs");
        request.setAcks((short) -1);
        request.setTimeoutMs(5000);
        request.setTransactionalId("tx-1");

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.acks()).isEqualTo((short) -1);
            assertThat(sub.timeoutMs()).isEqualTo(5000);
            assertThat(sub.transactionalId()).isEqualTo("tx-1");
        }
    }

    @Test
    void shouldExcludeUnroutableTopicsFromDecomposition() {
        var request = produceRequest("a.orders", "unknown.topic");

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topicData()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = produceRequest("unknown.one", "unknown.two");

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionData() {
        var request = new ProduceRequestData();
        var td = new TopicProduceData().setName("a.orders");
        td.partitionData().add(new PartitionProduceData().setIndex(0));
        td.partitionData().add(new PartitionProduceData().setIndex(1));
        request.topicData().add(td);

        Map<String, ProduceRequestData> parts = decomposer.decompose(request, table);

        assertThat(findTopic(parts.get("route-a"), "a.orders").partitionData())
                .extracting("index")
                .containsExactly(0, 1);
    }

    @Test
    void shouldNotMutateOriginalRequest() {
        var request = produceRequest("a.orders", "b.logs");
        int originalSize = request.topicData().size();

        decomposer.decompose(request, table);

        assertThat(request.topicData()).hasSize(originalSize);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = produceRequest("a.orders", "b.logs");

        var respA = new ProduceResponseData();
        respA.responses().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new ProduceResponseData();
        respB.responses().add(topicResponse("b.logs", 0, Errors.NONE));

        ProduceResponseData merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.responses()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = produceRequest("a.orders", "b.logs");

        var respA = new ProduceResponseData().setThrottleTimeMs(100);
        respA.responses().add(topicResponse("a.orders", 0, Errors.NONE));
        var respB = new ProduceResponseData().setThrottleTimeMs(300);
        respB.responses().add(topicResponse("b.logs", 0, Errors.NONE));

        ProduceResponseData merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldPreservePartitionErrorCodes() {
        var request = produceRequest("a.orders");

        var resp = new ProduceResponseData();
        resp.responses().add(topicResponse("a.orders", 0, Errors.NOT_LEADER_OR_FOLLOWER));

        ProduceResponseData merged = decomposer.recompose(
                Map.of("route-a", resp), request);

        assertThat(findResponse(merged, "a.orders").partitionResponses().get(0).errorCode())
                .isEqualTo(Errors.NOT_LEADER_OR_FOLLOWER.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = new ProduceRequestData();
        var td = new TopicProduceData().setName("unknown.topic");
        td.partitionData().add(new PartitionProduceData().setIndex(0));
        td.partitionData().add(new PartitionProduceData().setIndex(1));
        request.topicData().add(td);

        ProduceResponseData error = ProduceDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).hasSize(1);
        var topicResp = findResponse(error, "unknown.topic");
        assertThat(topicResp).isNotNull();
        assertThat(topicResp.partitionResponses()).hasSize(2);
        assertThat(topicResp.partitionResponses()).allSatisfy(pr -> assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code()));
    }

    @Test
    void shouldOnlyIncludeUnroutableTopicsInErrorResponse() {
        var request = produceRequest("a.orders", "unknown.topic");

        ProduceResponseData error = ProduceDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).extracting("name")
                .containsExactly("unknown.topic");
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = produceRequest("a.orders", "b.logs");

        ProduceResponseData error = ProduceDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).isEmpty();
    }

    // --- helpers ---

    private static ProduceRequestData produceRequest(String... topicNames) {
        var request = new ProduceRequestData();
        for (var name : topicNames) {
            var td = new TopicProduceData().setName(name);
            td.partitionData().add(new PartitionProduceData().setIndex(0));
            request.topicData().add(td);
        }
        return request;
    }

    private static TopicProduceResponse topicResponse(String name,
                                                      int partition,
                                                      Errors error) {
        var tr = new TopicProduceResponse().setName(name);
        tr.partitionResponses().add(
                new PartitionProduceResponse()
                        .setIndex(partition)
                        .setErrorCode(error.code()));
        return tr;
    }

    private static TopicProduceData findTopic(ProduceRequestData data,
                                              String name) {
        for (var td : data.topicData()) {
            if (td.name().equals(name)) {
                return td;
            }
        }
        throw new AssertionError("Topic not found: " + name);
    }

    private static TopicProduceResponse findResponse(ProduceResponseData data,
                                                     String name) {
        for (var tr : data.responses()) {
            if (tr.name().equals(name)) {
                return tr;
            }
        }
        throw new AssertionError("Topic response not found: " + name);
    }
}
