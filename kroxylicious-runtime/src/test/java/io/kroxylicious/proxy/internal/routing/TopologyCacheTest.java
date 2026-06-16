/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.List;

import org.apache.kafka.common.Uuid;
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
    class PutCoordinator {

        @Test
        void shouldCacheCoordinator() {
            // When
            cache.putCoordinator(ROUTE_A, (byte) 0, "my-group", 42);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isEqualTo(42);
        }

        @Test
        void shouldCacheTransactionCoordinator() {
            // When
            cache.putCoordinator(ROUTE_A, (byte) 1, "my-txn", 99);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 1, "my-txn")).isEqualTo(99);
        }

        @Test
        void shouldScopeCoordinatorsByRoute() {
            // When
            cache.putCoordinator(ROUTE_A, (byte) 0, "my-group", 10);
            cache.putCoordinator(ROUTE_B, (byte) 0, "my-group", 20);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isEqualTo(10);
            assertThat(cache.coordinatorFor(ROUTE_B, (byte) 0, "my-group")).isEqualTo(20);
        }

        @Test
        void shouldDistinguishKeyTypes() {
            // When
            cache.putCoordinator(ROUTE_A, (byte) 0, "my-id", 10);
            cache.putCoordinator(ROUTE_A, (byte) 1, "my-id", 20);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-id")).isEqualTo(10);
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 1, "my-id")).isEqualTo(20);
        }

        @Test
        void shouldUpdateExistingCoordinator() {
            // Given
            cache.putCoordinator(ROUTE_A, (byte) 0, "my-group", 10);

            // When
            cache.putCoordinator(ROUTE_A, (byte) 0, "my-group", 20);

            // Then
            assertThat(cache.coordinatorFor(ROUTE_A, (byte) 0, "my-group")).isEqualTo(20);
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
            cache.putCoordinator(ROUTE_A, (byte) 0, "grp", 10);
            cache.putCoordinator(ROUTE_B, (byte) 0, "grp", 20);

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
    class AuthorizationSafety {

        @Test
        void shouldRetainExistingEntriesWhenFilteredMetadataOmitsTopics() {
            // Given
            var fullResponse = new MetadataResponseData();
            addTopicPartition(fullResponse, "orders", 0, 42, List.of(42, 43), List.of(42));
            addTopicPartition(fullResponse, "payments", 0, 43, List.of(43, 44), List.of(43));
            cache.updateFromMetadata(ROUTE_A, fullResponse);

            // When: a filtered METADATA response (simulating ACL filtering) contains only "orders"
            var filteredResponse = metadataResponse("orders", 0, 44, List.of(42, 43), List.of(42));
            cache.updateFromMetadata(ROUTE_A, filteredResponse);

            // Then: "orders" leader updated, but "payments" is retained (not removed by the filtered response)
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(44);
            assertThat(cache.leaderFor("payments", 0)).isEqualTo(43);
        }

        @Test
        void shouldAccumulateTopicsFromMultipleFilteredResponses() {
            // Given: empty cache

            // When: two filtered responses arrive (simulating two connections with different ACLs)
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42), List.of(42)));
            cache.updateFromMetadata(ROUTE_A, metadataResponse("payments", 0, 43, List.of(43), List.of(43)));

            // Then: cache converges toward the union of both views
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(42);
            assertThat(cache.leaderFor("payments", 0)).isEqualTo(43);
        }

        @Test
        void shouldSkipTopicsWithAuthorizationError() {
            // Given: cache has "secret-topic" from a prior full METADATA response
            cache.updateFromMetadata(ROUTE_A, metadataResponse("secret-topic", 0, 42, List.of(42), List.of(42)));

            // When: a METADATA response arrives where "secret-topic" has TOPIC_AUTHORIZATION_FAILED
            var errorResponse = new MetadataResponseData();
            var errorTopic = new MetadataResponseTopic()
                    .setName("secret-topic")
                    .setErrorCode(Errors.TOPIC_AUTHORIZATION_FAILED.code());
            errorTopic.partitions().add(new MetadataResponsePartition()
                    .setPartitionIndex(0)
                    .setLeaderId(99)
                    .setErrorCode(Errors.NONE.code())
                    .setReplicaNodes(List.of(99))
                    .setIsrNodes(List.of(99)));
            errorResponse.topics().add(errorTopic);
            cache.updateFromMetadata(ROUTE_A, errorResponse);

            // Then: existing cache entry preserved (error topic skipped, not used to overwrite)
            assertThat(cache.leaderFor("secret-topic", 0)).isEqualTo(42);
        }

        @Test
        void shouldAccumulateTopicIdMappingsAcrossResponses() {
            // Given: empty cache
            var ordersId = Uuid.randomUuid();
            var paymentsId = Uuid.randomUuid();

            // When: two responses populate different topicId-to-name mappings
            cache.updateFromMetadata(ROUTE_A, metadataResponseWithTopicId("orders", ordersId, 0, 42));
            cache.updateFromMetadata(ROUTE_A, metadataResponseWithTopicId("payments", paymentsId, 0, 43));

            // Then: both mappings are present
            assertThat(cache.topicNameFor(ordersId)).isEqualTo("orders");
            assertThat(cache.topicNameFor(paymentsId)).isEqualTo("payments");
        }

        @Test
        void shouldUpdateLeaderWhenFullResponseFollowsFiltered() {
            // Given: filtered METADATA sets "orders" leader to 42
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42, 43), List.of(42)));

            // When: a full METADATA response arrives with the leader changed
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 43, List.of(42, 43), List.of(43)));

            // Then: cache reflects the updated leader (self-healing)
            assertThat(cache.leaderFor("orders", 0)).isEqualTo(43);
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
