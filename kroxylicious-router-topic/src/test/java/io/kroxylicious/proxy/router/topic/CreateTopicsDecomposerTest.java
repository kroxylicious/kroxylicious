/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.CreateTopicsRequestData;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableReplicaAssignment;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsResponseData;
import org.apache.kafka.common.message.CreateTopicsResponseData.CreatableTopicResult;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class CreateTopicsDecomposerTest {

    private final CreateTopicsDecomposer decomposer = CreateTopicsDecomposer.INSTANCE;

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
        var request = createTopicsRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting("name")
                .containsExactly("b.logs");
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = createTopicsRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).hasSize(2);
    }

    @Test
    void shouldCopyEnvelopeFields() {
        var request = createTopicsRequest("a.orders", "b.logs");
        request.setTimeoutMs(30000);
        request.setValidateOnly(true);

        var parts = decomposer.decompose(request, table, (short) 0);

        for (var sub : parts.values()) {
            assertThat(sub.timeoutMs()).isEqualTo(30000);
            assertThat(sub.validateOnly()).isTrue();
        }
    }

    @Test
    void shouldExcludeUnroutableTopics() {
        var request = createTopicsRequest("a.orders", "unknown.topic");

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.orders");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsUnroutable() {
        var request = createTopicsRequest("unknown.one", "unknown.two");

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldPreserveTopicConfig() {
        var request = new CreateTopicsRequestData();
        var topic = new CreatableTopic()
                .setName("a.orders")
                .setNumPartitions(3)
                .setReplicationFactor((short) 2);
        topic.configs().add(new CreatableTopicConfig()
                .setName("retention.ms")
                .setValue("86400000"));
        request.topics().add(topic);

        var parts = decomposer.decompose(request, table, (short) 0);

        var decomposed = parts.get("route-a").topics().iterator().next();
        assertThat(decomposed.numPartitions()).isEqualTo(3);
        assertThat(decomposed.replicationFactor()).isEqualTo((short) 2);
        assertThat(decomposed.configs()).hasSize(1);
        assertThat(decomposed.configs().iterator().next().name()).isEqualTo("retention.ms");
    }

    // --- recompose ---

    @Test
    void shouldMergeResponses() {
        var request = createTopicsRequest("a.orders", "b.logs");

        var respA = new CreateTopicsResponseData();
        respA.topics().add(topicResult("a.orders", Errors.NONE));
        var respB = new CreateTopicsResponseData();
        respB.topics().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var request = createTopicsRequest("a.orders", "b.logs");

        var respA = new CreateTopicsResponseData().setThrottleTimeMs(100);
        respA.topics().add(topicResult("a.orders", Errors.NONE));
        var respB = new CreateTopicsResponseData().setThrottleTimeMs(300);
        respB.topics().add(topicResult("b.logs", Errors.NONE));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB), request);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldPreserveErrorCodes() {
        var request = createTopicsRequest("a.orders");

        var resp = new CreateTopicsResponseData();
        resp.topics().add(topicResult("a.orders", Errors.TOPIC_ALREADY_EXISTS));

        var merged = decomposer.recompose(Map.of("route-a", resp), request);

        var topic = merged.topics().stream()
                .filter(t -> t.name().equals("a.orders"))
                .findFirst().orElseThrow();
        assertThat(topic.errorCode())
                .isEqualTo(Errors.TOPIC_ALREADY_EXISTS.code());
    }

    // --- error response for unroutable topics ---

    @Test
    void shouldSynthesiseErrorForUnroutableTopics() {
        var request = createTopicsRequest("unknown.topic");

        var error = CreateTopicsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).hasSize(1);
        var topicResult = error.topics().iterator().next();
        assertThat(topicResult.name()).isEqualTo("unknown.topic");
        assertThat(topicResult.errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shouldReturnEmptyErrorResponseWhenAllTopicsRoutable() {
        var request = createTopicsRequest("a.orders", "b.logs");

        var error = CreateTopicsDecomposer.errorResponseForUnroutableTopics(request, table);

        assertThat(error.topics()).isEmpty();
    }

    // --- assignment rejection ---

    @Test
    void shouldExcludeTopicsWithExplicitAssignments() {
        var request = new CreateTopicsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));
        request.topics().add(new CreatableTopic().setName("a.payments")
                .setNumPartitions(1).setReplicationFactor((short) 1));

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting("name")
                .containsExactly("a.payments");
    }

    @Test
    void shouldReturnEmptyMapWhenAllTopicsHaveAssignments() {
        var request = new CreateTopicsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));

        var parts = decomposer.decompose(request, table, (short) 0);

        assertThat(parts).isEmpty();
    }

    @Test
    void shouldSynthesiseErrorForTopicsWithAssignments() {
        var request = new CreateTopicsRequestData();
        request.topics().add(topicWithAssignments("a.orders"));

        var error = CreateTopicsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.topics()).hasSize(1);
        var topicResult = error.topics().iterator().next();
        assertThat(topicResult.name()).isEqualTo("a.orders");
        assertThat(topicResult.errorCode())
                .isEqualTo(Errors.INVALID_REPLICA_ASSIGNMENT.code());
        assertThat(topicResult.errorMessage())
                .isEqualTo(CreateTopicsDecomposer.ASSIGNMENTS_NOT_SUPPORTED_MESSAGE);
    }

    @Test
    void shouldNotErrorRoutableTopicsWithoutAssignments() {
        var request = createTopicsRequest("a.orders", "b.logs");

        var error = CreateTopicsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.topics()).isEmpty();
    }

    @Test
    void shouldNotErrorUnroutableTopicsWithAssignments() {
        var request = new CreateTopicsRequestData();
        request.topics().add(topicWithAssignments("unknown.topic"));

        var error = CreateTopicsDecomposer.errorResponseForTopicsWithAssignments(request, table);

        assertThat(error.topics()).isEmpty();
    }

    // --- helpers ---

    private static CreateTopicsRequestData createTopicsRequest(String... topicNames) {
        var request = new CreateTopicsRequestData();
        for (var name : topicNames) {
            request.topics().add(new CreatableTopic()
                    .setName(name)
                    .setNumPartitions(1)
                    .setReplicationFactor((short) 1));
        }
        return request;
    }

    private static CreatableTopic topicWithAssignments(String name) {
        var topic = new CreatableTopic()
                .setName(name)
                .setNumPartitions(-1)
                .setReplicationFactor((short) -1);
        topic.assignments().add(new CreatableReplicaAssignment()
                .setPartitionIndex(0)
                .setBrokerIds(List.of(0, 1, 2)));
        return topic;
    }

    private static CreatableTopicResult topicResult(String name,
                                                    Errors error) {
        return new CreatableTopicResult()
                .setName(name)
                .setErrorCode(error.code());
    }
}
