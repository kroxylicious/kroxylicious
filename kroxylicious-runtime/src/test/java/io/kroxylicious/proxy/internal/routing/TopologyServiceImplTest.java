/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.routing;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TopologyServiceImplTest {

    private static final String ROUTE_A = "route-a";
    private static final String ROUTE_B = "route-b";

    private TopologyCache cache;
    private TopologyServiceImpl service;

    @BeforeEach
    void setUp() {
        cache = new TopologyCache();
        service = new TopologyServiceImpl(cache);
    }

    @Nested
    class BindRequestSender {

        @Test
        void shouldThrowWhenSenderNotBound() {
            // When / Then
            assertThatThrownBy(() -> service.leaders(Map.of(ROUTE_A, Set.of("topic"))))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Nested
    class Leaders {

        @Test
        void shouldReturnFromCacheWithoutSendingMetadata() throws Exception {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42, 43), List.of(42)));
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests));

            // When
            var leaders = service.leaders(Map.of(ROUTE_A, Set.of("orders"))).toCompletableFuture().get();

            // Then
            assertThat(requests).isEmpty();
            assertThat(leaders.leaderOf("orders", 0)).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(42));
        }

        @Test
        void shouldSendMetadataForUncachedTopics() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                cache.updateFromMetadata(route, metadataResponse("orders", 0, 42, List.of(42), List.of(42)));
            }));

            // When
            var leaders = service.leaders(Map.of(ROUTE_A, Set.of("orders"))).toCompletableFuture().get();

            // Then
            assertThat(requests).hasSize(1);
            assertThat(requests.get(0).route()).isEqualTo(ROUTE_A);
            assertThat(requests.get(0).header().requestApiKey()).isEqualTo(ApiKeys.METADATA.id);
            assertThat(leaders.leaderOf("orders", 0)).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(42));
        }

        @Test
        void shouldFanOutAcrossRoutes() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                if (route.equals(ROUTE_A)) {
                    cache.updateFromMetadata(route, metadataResponse("orders", 0, 10, List.of(10), List.of(10)));
                }
                else {
                    cache.updateFromMetadata(route, metadataResponse("payments", 0, 20, List.of(20), List.of(20)));
                }
            }));

            // When
            var leaders = service.leaders(Map.of(
                    ROUTE_A, Set.of("orders"),
                    ROUTE_B, Set.of("payments"))).toCompletableFuture().get();

            // Then
            assertThat(requests).hasSize(2);
            assertThat(leaders.leaderOf("orders", 0)).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(10));
            assertThat(leaders.leaderOf("payments", 0)).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(20));
        }

        @Test
        void shouldReturnEmptyForUnresolvableLeader() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                var data = new MetadataResponseData();
                var topic = new MetadataResponseTopic()
                        .setName("orders")
                        .setErrorCode(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
                data.topics().add(topic);
                cache.updateFromMetadata(route, data);
            }));

            // When
            var leaders = service.leaders(Map.of(ROUTE_A, Set.of("orders"))).toCompletableFuture().get();

            // Then
            assertThat(leaders.leaderOf("orders", 0)).isEmpty();
        }
    }

    @Nested
    class Coordinators {

        @Test
        void shouldDiscoverCoordinatorViaTwoPhaseProtocol() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                if (header.requestApiKey() == ApiKeys.FIND_COORDINATOR.id) {
                    cache.putCoordinator(route, (byte) 0, "my-group", 42);
                }
            }));

            // When
            var coordinators = service.coordinators(ROUTE_A, (byte) 0, Set.of("my-group")).toCompletableFuture().get();

            // Then
            assertThat(requests).hasSize(2);
            assertThat(requests.get(0).header().requestApiKey()).isEqualTo(ApiKeys.METADATA.id);
            assertThat(requests.get(1).header().requestApiKey()).isEqualTo(ApiKeys.FIND_COORDINATOR.id);
            assertThat(coordinators.coordinatorFor("my-group")).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(42));
        }

        @Test
        void shouldSetKeyDirectlyForSingleKey() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                if (header.requestApiKey() == ApiKeys.FIND_COORDINATOR.id) {
                    cache.putCoordinator(route, (byte) 0, "my-group", 42);
                }
            }));

            // When
            service.coordinators(ROUTE_A, (byte) 0, Set.of("my-group")).toCompletableFuture().get();

            // Then
            var fcRequest = (org.apache.kafka.common.message.FindCoordinatorRequestData) requests.get(1).request();
            assertThat(fcRequest.key()).isEqualTo("my-group");
            assertThat(fcRequest.coordinatorKeys()).isEmpty();
        }

        @Test
        void shouldUseBatchedKeysForMultipleKeys() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                if (header.requestApiKey() == ApiKeys.FIND_COORDINATOR.id) {
                    cache.putCoordinator(route, (byte) 0, "group-1", 10);
                    cache.putCoordinator(route, (byte) 0, "group-2", 20);
                }
            }));

            // When
            var coordinators = service.coordinators(ROUTE_A, (byte) 0, Set.of("group-1", "group-2")).toCompletableFuture().get();

            // Then
            var fcRequest = (org.apache.kafka.common.message.FindCoordinatorRequestData) requests.get(1).request();
            assertThat(fcRequest.coordinatorKeys()).containsExactlyInAnyOrder("group-1", "group-2");
            assertThat(coordinators.coordinatorFor("group-1")).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(10));
            assertThat(coordinators.coordinatorFor("group-2")).isPresent()
                    .get().isEqualTo(new VirtualNodeImpl(20));
        }

        @Test
        void shouldThrowOnCoordinatorError() {
            // Given
            service.bindRequestSender((route, header, request) -> {
                if (header.requestApiKey() == ApiKeys.FIND_COORDINATOR.id) {
                    return CompletableFuture.completedFuture(
                            new FindCoordinatorResponseData()
                                    .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code()));
                }
                return CompletableFuture.completedFuture(new MetadataResponseData());
            });

            // When / Then
            assertThatThrownBy(() -> service.coordinators(ROUTE_A, (byte) 0, Set.of("my-group")).toCompletableFuture().get())
                    .hasCauseInstanceOf(CoordinatorDiscoveryException.class);
        }
    }

    @Nested
    class TopicNames {

        @Test
        void shouldReturnFromCacheWithoutSendingMetadata() throws Exception {
            // Given
            var topicId = Uuid.randomUuid();
            cache.updateFromMetadata(ROUTE_A, metadataResponseWithTopicId("orders", topicId, 0, 42));
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests));

            // When
            var result = service.topicNames(ROUTE_A, Set.of(topicId)).toCompletableFuture().get();

            // Then
            assertThat(requests).isEmpty();
            assertThat(result).containsEntry(topicId, "orders");
        }

        @Test
        void shouldSendMetadataForUncachedTopicIds() throws Exception {
            // Given
            var topicId = Uuid.randomUuid();
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                cache.updateFromMetadata(route, metadataResponseWithTopicId("orders", topicId, 0, 42));
            }));

            // When
            var result = service.topicNames(ROUTE_A, Set.of(topicId)).toCompletableFuture().get();

            // Then
            assertThat(requests).hasSize(1);
            assertThat(requests.get(0).header().requestApiKey()).isEqualTo(ApiKeys.METADATA.id);
            var mdReq = (MetadataRequestData) requests.get(0).request();
            assertThat(mdReq.topics()).hasSize(1);
            assertThat(mdReq.topics().get(0).topicId()).isEqualTo(topicId);
            assertThat(result).containsEntry(topicId, "orders");
        }

        @Test
        void shouldReturnPartialResultsWhenSomeTopicIdsUnresolvable() throws Exception {
            // Given
            var knownId = Uuid.randomUuid();
            var unknownId = Uuid.randomUuid();
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                cache.updateFromMetadata(route, metadataResponseWithTopicId("orders", knownId, 0, 42));
            }));

            // When
            var result = service.topicNames(ROUTE_A, Set.of(knownId, unknownId)).toCompletableFuture().get();

            // Then
            assertThat(result).containsEntry(knownId, "orders");
            assertThat(result).doesNotContainKey(unknownId);
        }

        @Test
        void shouldNotSendMetadataForEmptyInput() throws Exception {
            // Given
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests));

            // When
            var result = service.topicNames(ROUTE_A, Set.of()).toCompletableFuture().get();

            // Then
            assertThat(requests).isEmpty();
            assertThat(result).isEmpty();
        }

        @Test
        void shouldOnlySendMetadataForUncachedIds() throws Exception {
            // Given
            var cachedId = Uuid.randomUuid();
            var uncachedId = Uuid.randomUuid();
            cache.updateFromMetadata(ROUTE_A, metadataResponseWithTopicId("cached-topic", cachedId, 0, 42));
            var requests = new ArrayList<RecordedRequest>();
            service.bindRequestSender(recordingSender(requests, (route, header, req) -> {
                cache.updateFromMetadata(route, metadataResponseWithTopicId("uncached-topic", uncachedId, 0, 43));
            }));

            // When
            var result = service.topicNames(ROUTE_A, Set.of(cachedId, uncachedId)).toCompletableFuture().get();

            // Then
            assertThat(requests).hasSize(1);
            var mdReq = (MetadataRequestData) requests.get(0).request();
            assertThat(mdReq.topics()).hasSize(1);
            assertThat(mdReq.topics().get(0).topicId()).isEqualTo(uncachedId);
            assertThat(result).containsEntry(cachedId, "cached-topic");
            assertThat(result).containsEntry(uncachedId, "uncached-topic");
        }
    }

    @Nested
    class PartitionInfoLookup {

        @Test
        void shouldReturnCachedPartitionInfo() {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42, 43, 44), List.of(42, 43)));

            // When
            var info = service.partitionInfo("orders", 0);

            // Then
            assertThat(info).isPresent();
            assertThat(info.get().leader()).isEqualTo(new VirtualNodeImpl(42));
            assertThat(info.get().replicas()).containsExactly(new VirtualNodeImpl(42), new VirtualNodeImpl(43), new VirtualNodeImpl(44));
            assertThat(info.get().isr()).containsExactly(new VirtualNodeImpl(42), new VirtualNodeImpl(43));
        }

        @Test
        void shouldReturnEmptyForUncachedPartition() {
            // When
            var info = service.partitionInfo("nonexistent", 0);

            // Then
            assertThat(info).isEmpty();
        }
    }

    @Nested
    class BrokerInfoLookup {

        @Test
        void shouldReturnCachedBrokerInfo() {
            // Given
            var data = new MetadataResponseData();
            data.brokers().add(new MetadataResponseBroker()
                    .setNodeId(42).setHost("broker-a").setPort(9092).setRack("us-east-1a"));
            cache.updateFromMetadata(ROUTE_A, data);

            // When
            var info = service.brokerInfo(new VirtualNodeImpl(42));

            // Then
            assertThat(info).isPresent();
            assertThat(info.get().host()).isEqualTo("broker-a");
            assertThat(info.get().port()).isEqualTo(9092);
            assertThat(info.get().rack()).isEqualTo("us-east-1a");
        }

        @Test
        void shouldReturnEmptyForUnknownNode() {
            // When
            var info = service.brokerInfo(new VirtualNodeImpl(999));

            // Then
            assertThat(info).isEmpty();
        }
    }

    @Nested
    class InvalidateRouteLookup {

        @Test
        void shouldDelegateToCacheInvalidation() {
            // Given
            cache.updateFromMetadata(ROUTE_A, metadataResponse("orders", 0, 42, List.of(42), List.of(42)));

            // When
            service.invalidateRoute(ROUTE_A);

            // Then
            assertThat(cache.leaderFor("orders", 0)).isNull();
        }
    }

    // --- helpers ---

    record RecordedRequest(String route, RequestHeaderData header, ApiMessage request) {}

    @FunctionalInterface
    interface SenderSideEffect {
        void apply(String route, RequestHeaderData header, ApiMessage request);
    }

    private TopologyServiceImpl.RequestSender recordingSender(List<RecordedRequest> requests) {
        return recordingSender(requests, (route, header, req) -> {
        });
    }

    private TopologyServiceImpl.RequestSender recordingSender(List<RecordedRequest> requests, SenderSideEffect sideEffect) {
        return (route, header, request) -> {
            requests.add(new RecordedRequest(route, header, request));
            sideEffect.apply(route, header, request);
            if (header.requestApiKey() == ApiKeys.FIND_COORDINATOR.id) {
                return CompletableFuture.completedFuture(new FindCoordinatorResponseData());
            }
            return CompletableFuture.completedFuture(new MetadataResponseData());
        };
    }

    private static MetadataResponseData metadataResponse(String topicName, int partition, int leader,
                                                         List<Integer> replicas, List<Integer> isr) {
        var data = new MetadataResponseData();
        var topic = new MetadataResponseTopic().setName(topicName).setErrorCode(Errors.NONE.code());
        topic.partitions().add(new MetadataResponsePartition()
                .setPartitionIndex(partition)
                .setLeaderId(leader)
                .setErrorCode(Errors.NONE.code())
                .setReplicaNodes(replicas)
                .setIsrNodes(isr));
        data.topics().add(topic);
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
}
