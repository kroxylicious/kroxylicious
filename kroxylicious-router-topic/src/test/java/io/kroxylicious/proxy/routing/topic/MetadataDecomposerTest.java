/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.Test;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class MetadataDecomposerTest {

    private static final String DEFAULT_ROUTE = "route-a";

    private final MetadataDecomposer decomposer = MetadataDecomposer.INSTANCE;

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
            return DEFAULT_ROUTE;
        }

        @Override
        public Set<String> allRoutes() {
            return Set.of("route-a", "route-b");
        }
    };

    // --- decompose ---

    @Test
    void shouldSendAllTopicsRequestToAllRoutes() {
        var request = new MetadataRequestData().setTopics(null);

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        for (var sub : parts.values()) {
            assertThat(sub.topics()).isNull();
        }
    }

    @Test
    void shouldSendEmptyTopicsRequestToDefaultRoute() {
        var request = new MetadataRequestData();

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(parts).containsOnlyKeys(DEFAULT_ROUTE);
        assertThat(parts.get(DEFAULT_ROUTE).topics()).isEmpty();
    }

    @Test
    void shouldPartitionSpecificTopicsByRoute() {
        var request = metadataRequest("a.orders", "b.logs", "a.payments");

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(parts).containsOnlyKeys("route-a", "route-b");
        assertThat(parts.get("route-a").topics()).extracting(MetadataRequestTopic::name)
                .containsExactly("a.orders", "a.payments");
        assertThat(parts.get("route-b").topics()).extracting(MetadataRequestTopic::name)
                .containsExactly("b.logs");
    }

    @Test
    void shouldSendUnmatchedTopicToDefaultRoute() {
        var request = metadataRequest("unknown-topic", "a.orders");

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(parts).containsOnlyKeys("route-a");
        assertThat(parts.get("route-a").topics()).extracting(MetadataRequestTopic::name)
                .containsExactly("unknown-topic", "a.orders");
    }

    @Test
    void shouldPreserveEnvelopeFields() {
        var request = metadataRequest("a.orders", "b.logs");
        request.setAllowAutoTopicCreation(false);
        request.setIncludeClusterAuthorizedOperations(true);
        request.setIncludeTopicAuthorizedOperations(true);

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        for (var sub : parts.values()) {
            assertThat(sub.allowAutoTopicCreation()).isFalse();
            assertThat(sub.includeClusterAuthorizedOperations()).isTrue();
            assertThat(sub.includeTopicAuthorizedOperations()).isTrue();
        }
    }

    @Test
    void shouldNotMutateOriginalRequest() {
        var request = metadataRequest("a.orders", "b.logs");
        int originalSize = request.topics().size();

        decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(request.topics()).hasSize(originalSize);
    }

    @Test
    void shouldReturnSingleEntryWhenAllTopicsOnOneRoute() {
        var request = metadataRequest("a.orders", "a.payments");

        var parts = decomposer.decompose(request, table, DEFAULT_ROUTE);

        assertThat(parts).containsOnlyKeys("route-a");
    }

    // --- recompose ---

    @Test
    void shouldUnionBrokersByNodeId() {
        var respA = metadataResponse(
                List.of(broker(0, "host-a", 9092), broker(1, "host-a", 9093)),
                List.of());
        var respB = metadataResponse(
                List.of(broker(0, "host-b", 9092), broker(2, "host-b", 9094)),
                List.of());

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                metadataRequest("a.x", "b.y"), table, DEFAULT_ROUTE);

        assertThat(merged.brokers()).hasSize(3);
        assertThat(merged.brokers()).extracting(MetadataResponseBroker::nodeId)
                .containsExactlyInAnyOrder(0, 1, 2);
        assertThat(merged.brokers().find(0).host())
                .as("first seen broker for nodeId 0 should win")
                .isEqualTo("host-a");
    }

    @Test
    void shouldMergeTopicsFromMultipleRoutes() {
        var respA = metadataResponse(List.of(), List.of(topicMetadata("a.orders", 0)));
        var respB = metadataResponse(List.of(), List.of(topicMetadata("b.logs", 0)));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                metadataRequest("a.orders", "b.logs"), table, DEFAULT_ROUTE);

        assertThat(merged.topics()).extracting(MetadataResponseTopic::name)
                .containsExactlyInAnyOrder("a.orders", "b.logs");
    }

    @Test
    void shouldFilterPhantomTopicsForAllTopicsRequest() {
        var respA = metadataResponse(
                List.of(),
                List.of(topicMetadata("a.real", 0), topicMetadata("b.phantom", 0)));
        var respB = metadataResponse(
                List.of(),
                List.of(topicMetadata("b.real", 0), topicMetadata("a.phantom", 0)));

        var allTopicsRequest = new MetadataRequestData().setTopics(null);

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                allTopicsRequest, table, DEFAULT_ROUTE);

        assertThat(merged.topics()).extracting(MetadataResponseTopic::name)
                .containsExactlyInAnyOrder("a.real", "b.real");
    }

    @Test
    void shouldNotFilterTopicsForSpecificTopicsRequest() {
        var respA = metadataResponse(List.of(), List.of(topicMetadata("a.orders", 0)));
        var respB = metadataResponse(List.of(), List.of(topicMetadata("b.logs", 0)));

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                metadataRequest("a.orders", "b.logs"), table, DEFAULT_ROUTE);

        assertThat(merged.topics()).hasSize(2);
    }

    @Test
    void shouldUseDefaultRouteClusterMetadata() {
        var respA = metadataResponse(List.of(), List.of());
        respA.setClusterId("cluster-a-id");
        respA.setControllerId(1);
        respA.setClusterAuthorizedOperations(42);

        var respB = metadataResponse(List.of(), List.of());
        respB.setClusterId("cluster-b-id");
        respB.setControllerId(2);
        respB.setClusterAuthorizedOperations(99);

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                metadataRequest("a.x"), table, DEFAULT_ROUTE);

        assertThat(merged.clusterId()).isEqualTo("cluster-a-id");
        assertThat(merged.controllerId()).isEqualTo(1);
        assertThat(merged.clusterAuthorizedOperations()).isEqualTo(42);
    }

    @Test
    void shouldTakeMaxThrottleTime() {
        var respA = metadataResponse(List.of(), List.of()).setThrottleTimeMs(100);
        var respB = metadataResponse(List.of(), List.of()).setThrottleTimeMs(300);

        var merged = decomposer.recompose(
                Map.of("route-a", respA, "route-b", respB),
                metadataRequest("a.x"), table, DEFAULT_ROUTE);

        assertThat(merged.throttleTimeMs()).isEqualTo(300);
    }

    @Test
    void shouldPreserveTopicErrors() {
        var resp = metadataResponse(List.of(), List.of());
        var errorTopic = new MetadataResponseTopic()
                .setName("a.missing")
                .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
        resp.topics().add(errorTopic);

        var merged = decomposer.recompose(
                Map.of("route-a", resp),
                metadataRequest("a.missing"), table, DEFAULT_ROUTE);

        assertThat(merged.topics().find("a.missing").errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shouldPreservePartitionMetadata() {
        var partition = new MetadataResponsePartition()
                .setPartitionIndex(0)
                .setLeaderId(1)
                .setReplicaNodes(List.of(1, 2))
                .setIsrNodes(List.of(1));
        var topic = new MetadataResponseTopic().setName("a.orders");
        topic.partitions().add(partition);

        var resp = metadataResponse(
                List.of(broker(1, "host", 9092), broker(2, "host", 9093)),
                List.of());
        resp.topics().add(topic);

        var merged = decomposer.recompose(
                Map.of("route-a", resp),
                metadataRequest("a.orders"), table, DEFAULT_ROUTE);

        var mergedTopic = merged.topics().find("a.orders");
        assertThat(mergedTopic.partitions()).hasSize(1);
        var p = mergedTopic.partitions().get(0);
        assertThat(p.partitionIndex()).isEqualTo(0);
        assertThat(p.leaderId()).isEqualTo(1);
        assertThat(p.replicaNodes()).containsExactly(1, 2);
        assertThat(p.isrNodes()).containsExactly(1);
    }

    // --- helpers ---

    private static MetadataRequestData metadataRequest(String... topicNames) {
        var request = new MetadataRequestData();
        var topics = new ArrayList<MetadataRequestTopic>();
        for (var name : topicNames) {
            topics.add(new MetadataRequestTopic().setName(name));
        }
        request.setTopics(topics);
        return request;
    }

    private static MetadataResponseBroker broker(int nodeId,
                                                 String host,
                                                 int port) {
        return new MetadataResponseBroker()
                .setNodeId(nodeId)
                .setHost(host)
                .setPort(port);
    }

    private static MetadataResponseTopic topicMetadata(String name,
                                                       int leaderId) {
        var topic = new MetadataResponseTopic()
                .setName(name)
                .setErrorCode(Errors.NONE.code());
        topic.partitions().add(new MetadataResponsePartition()
                .setPartitionIndex(0)
                .setLeaderId(leaderId));
        return topic;
    }

    private static MetadataResponseData metadataResponse(
                                                         List<MetadataResponseBroker> brokers,
                                                         List<MetadataResponseTopic> topics) {
        var resp = new MetadataResponseData();
        var brokerCollection = new MetadataResponseBrokerCollection();
        brokers.forEach(b -> brokerCollection.add(b.duplicate()));
        resp.setBrokers(brokerCollection);
        var topicCollection = new MetadataResponseTopicCollection();
        topics.forEach(t -> topicCollection.add(t.duplicate()));
        resp.setTopics(topicCollection);
        return resp;
    }
}
