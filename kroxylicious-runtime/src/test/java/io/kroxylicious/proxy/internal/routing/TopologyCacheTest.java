/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.FindCoordinatorResponseData.Coordinator;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TopologyCacheTest {

    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";

    private TopologyCache cache;

    @BeforeEach
    void setUp() {
        cache = new TopologyCache();
    }

    @Nested
    class UpdateFromMetadata {

        @Test
        void shouldCachePartitionLeader() {
            // Given
            var data = metadataResponse("orders", 0, 42, List.of(42, 43), List.of(42));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(42);
        }

        @Test
        void shouldCacheFullPartitionInfo() {
            // Given
            var data = metadataResponse("orders", 0, 42, List.of(42, 43, 44), List.of(42, 43));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            var info = cache.partitionInfoFor("orders", 0);
            assertThat(info).isNotNull();
            assertThat(info.leader()).isEqualTo(42);
            assertThat(info.replicas()).containsExactly(42, 43, 44);
            assertThat(info.isr()).containsExactly(42, 43);
        }

        @Test
        void shouldCacheBrokerInfo() {
            // Given
            var data = new MetadataResponseData();
            data.brokers().add(new MetadataResponseBroker()
                    .setNodeId(42).setHost("broker-a").setPort(9092).setRack("us-east-1a"));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            var broker = cache.brokerInfo(42);
            assertThat(broker).isNotNull();
            assertThat(broker.host()).isEqualTo("broker-a");
            assertThat(broker.port()).isEqualTo(9092);
            assertThat(broker.rack()).isEqualTo("us-east-1a");
        }

        @Test
        void shouldCacheBrokerInfoWithNullRack() {
            // Given
            var data = new MetadataResponseData();
            data.brokers().add(new MetadataResponseBroker()
                    .setNodeId(1).setHost("h").setPort(9092).setRack(null));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.brokerInfo(1).rack()).isNull();
        }

        @Test
        void shouldCacheTopicIdToNameMapping() {
            // Given
            var topicId = Uuid.randomUuid();
            var data = metadataResponseWithTopicId("orders", topicId, 0, 42);

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.topicNameFor(topicId)).isEqualTo("orders");
        }

        @Test
        void shouldSkipErrorPartitions() {
            // Given
            var data = new MetadataResponseData();
            var topic = new MetadataResponseTopic().setName("orders").setErrorCode(Errors.NONE.code());
            topic.partitions().add(new MetadataResponsePartition()
                    .setPartitionIndex(0)
                    .setLeaderId(42)
                    .setErrorCode(Errors.LEADER_NOT_AVAILABLE.code())
                    .setReplicaNodes(List.of(42))
                    .setIsrNodes(List.of(42)));
            data.topics().add(topic);

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isNull();
        }

        @Test
        void shouldSkipNegativeLeaderId() {
            // Given
            var data = metadataResponse("orders", 0, -1, List.of(42), List.of(42));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isNull();
        }

        @Test
        void shouldSkipErrorTopics() {
            // Given
            var data = new MetadataResponseData();
            var topic = new MetadataResponseTopic()
                    .setName("orders")
                    .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
            topic.partitions().add(new MetadataResponsePartition()
                    .setPartitionIndex(0)
                    .setLeaderId(42)
                    .setErrorCode(Errors.NONE.code())
                    .setReplicaNodes(List.of(42))
                    .setIsrNodes(List.of(42)));
            data.topics().add(topic);

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isNull();
        }

        @Test
        void shouldSkipZeroTopicId() {
            // Given
            var data = metadataResponseWithTopicId("orders", Uuid.ZERO_UUID, 0, 42);

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.topicNameFor(Uuid.ZERO_UUID)).isNull();
        }

        @Test
        void shouldUpdateExistingEntries() {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42, 43), List.of(42)));

            // When
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 43, List.of(42, 43), List.of(43)));

            // Then
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(43);
        }

        @Test
        void shouldCacheMultipleTopicsAndPartitions() {
            // Given
            var data = new MetadataResponseData();
            addTopicPartition(data, "orders", 0, 42, List.of(42, 43), List.of(42));
            addTopicPartition(data, "orders", 1, 43, List.of(42, 43), List.of(43));
            addTopicPartition(data, "payments", 0, 44, List.of(44), List.of(44));

            // When
            cache.updateFromMetadata(ROUTE_A, data);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(42);
            assertThat(cache.leaderFor("orders", 1)).isEqualTo(43);
            assertThat(cache.leaderFor("payments", 0)).isEqualTo(44);
        }
    }

    @Nested
    class UpdateFromFindCoordinator {

        @Test
        void shouldCacheCoordinatorV4() {
            // Given
            var data = new FindCoordinatorResponseData();
            data.coordinators().add(new Coordinator()
                    .setKey("my-group")
                    .setKeyType((byte) 0)
                    .setNodeId(42)
                    .setErrorCode(Errors.NONE.code()));

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 4);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isEqualTo(42);
        }

        @Test
        void shouldCacheCoordinatorPreV4() {
            // Given
            var data = new FindCoordinatorResponseData()
                    .setKey("my-txn")
                    .setKeyType((byte) 1)
                    .setNodeId(99)
                    .setErrorCode(Errors.NONE.code());

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 3);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 1, "my-txn")).isEqualTo(99);
        }

        @Test
        void shouldSkipErrorCoordinatorV4() {
            // Given
            var data = new FindCoordinatorResponseData();
            data.coordinators().add(new Coordinator()
                    .setKey("my-group")
                    .setKeyType((byte) 0)
                    .setNodeId(42)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 4);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isNull();
        }

        @Test
        void shouldSkipErrorCoordinatorPreV4() {
            // Given
            var data = new FindCoordinatorResponseData()
                    .setKey("my-group")
                    .setKeyType((byte) 0)
                    .setNodeId(42)
                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code());

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 3);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isNull();
        }

        @Test
        void shouldSkipNegativeNodeIdPreV4() {
            // Given
            var data = new FindCoordinatorResponseData()
                    .setKey("my-group")
                    .setKeyType((byte) 0)
                    .setNodeId(-1)
                    .setErrorCode(Errors.NONE.code());

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 3);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isNull();
        }

        @Test
        void shouldCacheMultipleCoordinatorsV4() {
            // Given
            var data = new FindCoordinatorResponseData();
            data.coordinators().add(new Coordinator()
                    .setKey("group-a").setKeyType((byte) 0).setNodeId(10).setErrorCode(Errors.NONE.code()));
            data.coordinators().add(new Coordinator()
                    .setKey("group-b").setKeyType((byte) 0).setNodeId(20).setErrorCode(Errors.NONE.code()));

            // When
            cache.updateFromFindCoordinator(ROUTE_A, data, (short) 4);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "group-a")).isEqualTo(10);
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "group-b")).isEqualTo(20);
        }

        @Test
        void shouldScopeCoordinatorsByRoute() {
            // Given
            var dataA = new FindCoordinatorResponseData()
                    .setKey("my-group").setKeyType((byte) 0).setNodeId(10).setErrorCode(Errors.NONE.code());
            var dataB = new FindCoordinatorResponseData()
                    .setKey("my-group").setKeyType((byte) 0).setNodeId(20).setErrorCode(Errors.NONE.code());

            // When
            cache.updateFromFindCoordinator(ROUTE_A, dataA, (short) 3);
            cache.updateFromFindCoordinator(ROUTE_B, dataB, (short) 3);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isEqualTo(10);
            assertThat(cache.coordinatorFor(ROUTE_B, (byte) 0, "my-group")).isEqualTo(20);
        }
    }

    @Nested
    class InvalidateRoute {

        @Test
        void shouldClearPartitionsForRoute() {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42), List.of(42)));
            cache.updateFromMetadata(ROUTE_B, metadataResponse("payments", 0, 99, List.of(99), List.of(99)));

            // When
            cache.invalidateRoute(ROUTE_A);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isNull();
            assertThat(cache.leaderFor("payments", 0)).isEqualTo(99);
        }

        @Test
        void shouldClearCoordinatorsForRoute() {
            // Given
            var dataA = new FindCoordinatorResponseData()
                    .setKey("grp").setKeyType((byte) 0).setNodeId(10).setErrorCode(Errors.NONE.code());
            var dataB = new FindCoordinatorResponseData()
                    .setKey("grp").setKeyType((byte) 0).setNodeId(20).setErrorCode(Errors.NONE.code());
            cache.updateFromFindCoordinator(ROUTE_A, dataA, (short) 3);
            cache.updateFromFindCoordinator(ROUTE_B, dataB, (short) 3);

            // When
            cache.invalidateRoute(ROUTE_A);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "grp")).isNull();
            assertThat(cache.coordinatorFor(ROUTE_B, (byte) 0, "grp")).isEqualTo(20);
        }

        @Test
        void shouldClearBrokerInfo() {
            // Given
            var data = new MetadataResponseData();
            data.brokers().add(new MetadataResponseBroker().setNodeId(42).setHost("h").setPort(9092));
            cache.updateFromMetadata(ROUTE_A, data);

            // When
            cache.invalidateRoute(ROUTE_A);

            // Then
            assertThat(cache.brokerInfo(42)).isNull();
        }

        @Test
        void shouldNotClearTopicNames() {
            // Given
            var topicId = Uuid.randomUuid();
            cache.updateFromMetadata(ROUTE_A, metadataResponseWithTopicId("orders", topicId, 0, 42));

            // When
            cache.invalidateRoute(ROUTE_A);

            // Then
            assertThat(cache.topicNameFor(topicId)).isEqualTo("orders");
        }

        @Test
        void shouldHandleInvalidatingUnknownRoute() {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42), List.of(42)));

            // When
            cache.invalidateRoute("unknown-route");

            // Then
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(42);
        }
    }

    @Nested
    class LookupMisses {

        @Test
        void shouldReturnNullForUncachedPartition() {
            assertThat(cache.leaderFor("nonexistent", 0)).isNull();
            assertThat(cache.partitionInfoFor("nonexistent", 0)).isNull();
        }

        @Test
        void shouldReturnNullForUncachedBroker() {
            assertThat(cache.brokerInfo(999)).isNull();
        }

        @Test
        void shouldReturnNullForUncachedCoordinator() {
            assertThat(cache.coordinatorFor("route", (byte) 0, "unknown")).isNull();
        }

        @Test
        void shouldReturnNullForUncachedTopicName() {
            assertThat(cache.topicNameFor(Uuid.randomUuid())).isNull();
        }
    }

    // --- helpers ---

    private static MetadataResponseData metadataResponse(String topicName, int partition, int leader,
                                                         List<Integer> replicas, List<Integer> isr) {
        var data = new MetadataResponseData();
        addTopicPartition(data, topicName, partition, leader, replicas, isr);
        return data;
    }

    private static MetadataResponseData metadataResponseWithTopicId(String topicName, Uuid topicId,
                                                                    int partition, int leader) {
        var data = new MetadataResponseData();
        var topic = new MetadataResponseTopic()
                .setName(topicName)
                .setTopicId(topicId)
                .setErrorCode(Errors.NONE.code());
        topic.partitions().add(new MetadataResponsePartition()
                .setPartitionIndex(partition)
                .setLeaderId(leader)
                .setErrorCode(Errors.NONE.code())
                .setReplicaNodes(List.of(leader))
                .setIsrNodes(List.of(leader)));
        data.topics().add(topic);
        return data;
    }

    private static void addTopicPartition(MetadataResponseData data, String topicName,
                                          int partition, int leader,
                                          List<Integer> replicas, List<Integer> isr) {
        MetadataResponseTopic topic = null;
        for (var t : data.topics()) {
            if (t.name().equals(topicName)) {
                topic = t;
                break;
            }
        }
        if (topic == null) {
            topic = new MetadataResponseTopic().setName(topicName).setErrorCode(Errors.NONE.code());
            data.topics().add(topic);
        }
        topic.partitions().add(new MetadataResponsePartition()
                .setPartitionIndex(partition)
                .setLeaderId(leader)
                .setErrorCode(Errors.NONE.code())
                .setReplicaNodes(replicas)
                .setIsrNodes(isr));
    }
}
