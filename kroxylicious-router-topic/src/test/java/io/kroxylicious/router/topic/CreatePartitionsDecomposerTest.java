/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.router.topic;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.CreatePartitionsRequestData;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsAssignment;
import org.apache.kafka.common.message.CreatePartitionsRequestData.CreatePartitionsTopic;
import org.apache.kafka.common.message.CreatePartitionsResponseData;
import org.apache.kafka.common.message.CreatePartitionsResponseData.CreatePartitionsTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class CreatePartitionsDecomposerTest {

    private final CreatePartitionsDecomposer decomposer = CreatePartitionsDecomposer.INSTANCE;

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
        var request = createPartitionsRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = createPartitionsRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = createPartitionsRequest("a.orders", "b.logs");
        request.setTimeoutMs(20000);
        request.setValidateOnly(true);

        var parts = decomposer.decompose(request, table);

        for (var sub : parts.values()) {
            assertThat(sub.timeoutMs()).isEqualTo(20000);
            assertThat(sub.validateOnly()).isTrue();
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = createPartitionsRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = createPartitionsRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreservePartitionCount() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(new CreatePartitionsTopic()
                .setName("a.orders")
                .setCount(6));

        var parts = decomposer.decompose(request, table);

        assertThat(parts.get("route-a").topics().iterator().next().count()).isEqualTo(6);
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = createPartitionsRequest("a.orders", "b.logs");

        var respA = new CreatePartitionsResponseData();
        respA.results().add(topicResult("a.orders", Errors.NONE));
        var respB = new CreatePartitionsResponseData();
        respB.results().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.results()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = createPartitionsRequest("a.orders", "b.logs");

        var respA = new CreatePartitionsResponseData().setThrottleTimeMs(150);
        respA.results().add(topicResult("a.orders", Errors.NONE));
        var respB = new CreatePartitionsResponseData().setThrottleTimeMs(400);
        respB.results().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(400);
    }

    @Test
    void shouldPreserveErrorCodes() {
        var request = createPartitionsRequest("a.orders");

        var resp = new CreatePartitionsResponseData();
        resp.results().add(topicResult("a.orders", Errors.INVALID_PARTITIONS));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var topic = merged.results().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(topic.errorCode())
                .isEqualTo(Errors.INVALID_PARTITIONS.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = createPartitionsRequest("unknown.topic");

        var error = CreatePartitionsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.results()).hasSize(1);
        var topicResult = error.results().iterator().next();
        assertThat(topicResult.name()).isEqualTo("unknown.topic");
        assertThat(topicResult.errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = createPartitionsRequest("a.orders", "b.logs");

        var error = CreatePartitionsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.results()).isEmpty();
    }

    // --- assignment rejection ---

    @Test
    void shouldExcludeTopicsWithExplicitAssignments() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));
        request.topics().add(new CreatePartitionsTopic()
                .setName("a.payments").setCount(3).setAssignments(null));

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.payments");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsHaveAssignments() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));

        var parts = decomposer.decompose(request, table);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldSynthesiseErrorForTopicsWithAssignments() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));

        var error = CreatePartitionsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.results()).hasSize(1);
        var topicResult = error.results().iterator().next();
        assertThat(topicResult.name()).isEqualTo("a.orders");
        assertThat(topicResult.errorCode())
                .isEqualTo(Errors.INVALID_REPLICA_ASSIGNMENT.code());
        assertThat(topicResult.errorMessage())
                .isEqualTo(CreatePartitionsDecomposer.ASSIGNMENTS_NOT_SUPPORTED_MESSAGE);
    }

    @Test
    void shouldNotErrorRoutableTopicsWithoutAssignments() {
        var request = createPartitionsRequest("a.orders", "b.logs");

        var error = CreatePartitionsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.results()).isEmpty();
    }

    @Test
    void shouldNotErrorUnroutableTopicsWithAssignments() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(topicWithAssignments("unknown.topic"));

        var error = CreatePartitionsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.results()).isEmpty();
    }

    @Test
    void shouldTreatNullAssignmentsAsAutomatic() {
        var request = new CreatePartitionsRequestData();
        request.topics().add(new CreatePartitionsTopic()
                .setName("a.orders").setCount(3).setAssignments(null));

        var parts = decomposer.decompose(request, table);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    // --- helpers ---

    private static CreatePartitionsTopic topicWithAssignments(String name) {
        return new CreatePartitionsTopic()
                .setName(name)
                .setCount(3)
                .setAssignments(List.of(
                        new CreatePartitionsAssignment().setBrokerIds(List.of(0, 1, 2))));
    }

    private static CreatePartitionsRequestData createPartitionsRequest(String... topicNames) {
        var request = new CreatePartitionsRequestData();
        for (var name : topicNames) {
            request.topics().add(new CreatePartitionsTopic()
                    .setName(name)
                    .setCount(3)
                    .setAssignments(null));
        }
        return request;
    }

    private static CreatePartitionsTopicResult topicResult(String name,
                                                           Errors error) {
        return new CreatePartitionsTopicResult()
                .setName(name)
                .setErrorCode(error.code());
    }
}
