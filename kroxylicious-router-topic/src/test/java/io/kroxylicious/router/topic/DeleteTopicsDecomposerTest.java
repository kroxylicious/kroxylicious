/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.DeleteTopicsRequestData;
import org.apache.kafka.common.message.DeleteTopicsResponseData;
import org.apache.kafka.common.message.DeleteTopicsResponseData.DeletableTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class DeleteTopicsDecomposerTest {

    private final DeleteTopicsDecomposer decomposer = DeleteTopicsDecomposer.INSTANCE;

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
        var request = deleteTopicsRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topicNames())
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topicNames())
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = deleteTopicsRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topicNames()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = deleteTopicsRequest("a.orders", "b.logs");
        request.setTimeoutMs(45000);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.timeoutMs()).isEqualTo(45000);
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = deleteTopicsRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topicNames())
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = deleteTopicsRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = deleteTopicsRequest("a.orders", "b.logs");

        var respA = new DeleteTopicsResponseData();
        respA.responses().add(topicResult("a.orders", Errors.NONE));
        var respB = new DeleteTopicsResponseData();
        respB.responses().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.responses()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = deleteTopicsRequest("a.orders", "b.logs");

        var respA = new DeleteTopicsResponseData().setThrottleTimeMs(50);
        respA.responses().add(topicResult("a.orders", Errors.NONE));
        var respB = new DeleteTopicsResponseData().setThrottleTimeMs(200);
        respB.responses().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(200);
    }

    @Test
    void shouldPreserveErrorCodes() {
        var request = deleteTopicsRequest("a.orders");

        var resp = new DeleteTopicsResponseData();
        resp.responses().add(topicResult("a.orders", Errors.TOPIC_DELETION_DISABLED));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var topic = merged.responses().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(topic.errorCode())
                .isEqualTo(Errors.TOPIC_DELETION_DISABLED.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = deleteTopicsRequest("unknown.topic");

        var error = DeleteTopicsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).hasSize(1);
        var topicResult = error.responses().iterator().next();
        assertThat(topicResult.name()).isEqualTo("unknown.topic");
        assertThat(topicResult.errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = deleteTopicsRequest("a.orders", "b.logs");

        var error = DeleteTopicsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.responses()).isEmpty();
    }

    // --- helpers ---

    private static DeleteTopicsRequestData deleteTopicsRequest(String... topicNames) {
        var request = new DeleteTopicsRequestData();
        for (var name : topicNames) {
            request.topicNames().add(name);
        }
        return request;
    }

    private static DeletableTopicResult topicResult(String name,
                                                    Errors error) {
        return new DeletableTopicResult()
                .setName(name)
                .setErrorCode(error.code());
    }
}
