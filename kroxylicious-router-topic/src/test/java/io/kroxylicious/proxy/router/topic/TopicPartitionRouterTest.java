/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.router.topic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData;
import org.apache.kafka.common.message.AddPartitionsToTxnResponseData;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
import org.apache.kafka.common.message.EndTxnRequestData;
import org.apache.kafka.common.message.EndTxnResponseData;
import org.apache.kafka.common.message.FetchRequestData;
import org.apache.kafka.common.message.FetchRequestData.FetchPartition;
import org.apache.kafka.common.message.FetchRequestData.FetchTopic;
import org.apache.kafka.common.message.FetchResponseData;
import org.apache.kafka.common.message.FetchResponseData.FetchableTopicResponse;
import org.apache.kafka.common.message.FetchResponseData.PartitionData;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.FindCoordinatorResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
import org.apache.kafka.common.message.ListOffsetsRequestData;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsPartition;
import org.apache.kafka.common.message.ListOffsetsRequestData.ListOffsetsTopic;
import org.apache.kafka.common.message.ListOffsetsResponseData;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsPartitionResponse;
import org.apache.kafka.common.message.ListOffsetsResponseData.ListOffsetsTopicResponse;
import org.apache.kafka.common.message.MetadataRequestData;
import org.apache.kafka.common.message.MetadataRequestData.MetadataRequestTopic;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBroker;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseBrokerCollection;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponsePartition;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic;
import org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopicCollection;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestPartition;
import org.apache.kafka.common.message.OffsetCommitRequestData.OffsetCommitRequestTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.ProduceRequestData;
import org.apache.kafka.common.message.ProduceRequestData.PartitionProduceData;
import org.apache.kafka.common.message.ProduceRequestData.TopicProduceData;
import org.apache.kafka.common.message.ProduceResponseData;
import org.apache.kafka.common.message.ProduceResponseData.PartitionProduceResponse;
import org.apache.kafka.common.message.ProduceResponseData.TopicProduceResponse;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.SimpleRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.router.BrokerInfo;
import io.kroxylicious.proxy.router.CloseOrTerminalStage;
import io.kroxylicious.proxy.router.Coordinators;
import io.kroxylicious.proxy.router.PartitionInfo;
import io.kroxylicious.proxy.router.PartitionLeaders;
import io.kroxylicious.proxy.router.RouterContext;
import io.kroxylicious.proxy.router.RouterResponse;
import io.kroxylicious.proxy.router.TerminalStage;
import io.kroxylicious.proxy.router.TopologyService;
import io.kroxylicious.proxy.router.VirtualNode;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class TopicPartitionRouterTest {

    record TestVirtualNode(int encodedId) implements VirtualNode {}

    static class TestTopologyService implements TopologyService {

        private final Map<String, Map<Integer, VirtualNode>> leaders = new HashMap<>();
        private final Map<String, VirtualNode> coordinatorMap = new HashMap<>();

        void primeLeader(String topicName, int partition, VirtualNode leader) {
            leaders.computeIfAbsent(topicName, k -> new HashMap<>()).put(partition, leader);
        }

        void primeCoordinator(String route, byte keyType, String key, VirtualNode node) {
            coordinatorMap.put(route + ":" + keyType + ":" + key, node);
        }

        @Override
        public CompletionStage<PartitionLeaders> leaders(Map<String, Set<String>> topicsByRoute) {
            PartitionLeaders snapshot = (topicName, partitionIndex) -> {
                var partMap = leaders.get(topicName);
                return partMap != null ? Optional.ofNullable(partMap.get(partitionIndex)) : Optional.empty();
            };
            return CompletableFuture.completedFuture(snapshot);
        }

        @Override
        public CompletionStage<Coordinators> coordinators(String route, byte keyType, Set<String> keys) {
            Coordinators snapshot = key -> {
                return Optional.ofNullable(coordinatorMap.get(route + ":" + keyType + ":" + key));
            };
            return CompletableFuture.completedFuture(snapshot);
        }

        @Override
        public CompletionStage<Map<Uuid, String>> topicNames(Set<Uuid> topicIds) {
            return CompletableFuture.completedFuture(Map.of());
        }

        @Override
        public Optional<PartitionInfo> partitionInfo(String topicName, int partitionIndex) {
            return Optional.empty();
        }

        @Override
        public Optional<BrokerInfo> brokerInfo(VirtualNode node) {
            return Optional.empty();
        }

        @Override
        public void invalidateRoute(String route) {
            // no-op in tests
        }
    }

    private TopicPartitionRouter router;
    private TestTopologyService testTopologyService;

    @BeforeEach
    void setUp() {
        testTopologyService = new TestTopologyService();
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "default-route");
        router = new TopicPartitionRouter(table, "default-route", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);
    }

    // --- close ---

    @Test
    void closeShouldReleaseFetchSessionCacheSlot() {
        var cache = new FetchSessionCache(1000, 10_000, "testVc", "testRouter");
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), "default-route");
        var closeable = new TopicPartitionRouter(table, "default-route", Map.of(),
                new ProducerIdManager(Duration.ofDays(7)), cache, Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = fetchRequest("orders.uk");
        request.setSessionId(0);
        request.setSessionEpoch(0);
        var backendResp = fetchResponse("orders.uk", 0, Errors.NONE);
        primeLeaderCache(closeable, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, backendResp));
        ctx.captureResult(closeable.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx).toCompletableFuture().join());
        assertThat(cache.size()).isEqualTo(1);

        closeable.close();

        assertThat(cache.size()).isEqualTo(0);
    }

    // --- static routes ---

    @Test
    void staticRoutesShouldNotContainDynamicallyRoutedKeys() {
        Map<ApiKeys, String> routes = router.staticRoutes();

        assertThat(routes).doesNotContainKey(ApiKeys.API_VERSIONS);
        assertThat(routes).doesNotContainKey(ApiKeys.PRODUCE);
        assertThat(routes).doesNotContainKey(ApiKeys.INIT_PRODUCER_ID);
    }

    @Test
    void staticRoutesShouldMapOtherKeysToDefault() {
        Map<ApiKeys, String> routes = router.staticRoutes();

        assertThat(routes).containsEntry(ApiKeys.HEARTBEAT, "default-route");
        assertThat(routes)
                .doesNotContainKey(ApiKeys.METADATA)
                .doesNotContainKey(ApiKeys.FETCH)
                .doesNotContainKey(ApiKeys.LIST_OFFSETS)
                .doesNotContainKey(ApiKeys.OFFSET_COMMIT)
                .doesNotContainKey(ApiKeys.FIND_COORDINATOR)
                .doesNotContainKey(ApiKeys.DESCRIBE_CLUSTER);
    }

    // --- API_VERSIONS capping ---

    @Test
    void shouldNotCapProduceVersionInApiVersionsResponse() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 13);

        var ctx = new CapturingRouterContext(responseData);
        ctx.captureResult(router.onRequest(
                (short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx).toCompletableFuture().join());

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.PRODUCE))
                .isEqualTo((short) 13);
    }

    @Test
    void shouldNotCapFetchVersionInApiVersionsResponse() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.FETCH, (short) 16);

        var ctx = new CapturingRouterContext(responseData);
        ctx.captureResult(router.onRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx).toCompletableFuture().join());

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.FETCH))
                .isEqualTo((short) 16);
    }

    @Test
    void shouldNotCapVersionsBelowLimit() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 10);

        var ctx = new CapturingRouterContext(responseData);
        ctx.captureResult(router.onRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx).toCompletableFuture().join());

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.PRODUCE))
                .isEqualTo((short) 10);
    }

    @Test
    void shouldRouteApiVersionsToVirtualNodeWhenPresent() {
        // Given
        var responseData = apiVersionsResponse();
        var ctx = new CapturingRouterContext(responseData)
                .withVirtualNodeId(42)
                .withNodeResponses(Map.of(42, apiVersionsResponse()));

        // When
        ctx.captureResult(router.onRequest(
                (short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(),
                responseData, ctx).toCompletableFuture().join());

        // Then
        assertThat(ctx.sentNodeRequests())
                .as("API_VERSIONS should be sent to the connected node")
                .hasSize(1);
        assertThat(ctx.sentNodeRequests().get(0).virtualNodeId()).isEqualTo(42);
        assertThat(ctx.sentRequests())
                .as("should not use anyNode path")
                .isEmpty();
    }

    @Test
    void shouldRouteApiVersionsToAnyNodeWhenVirtualNodeEmpty() {
        // Given
        var responseData = apiVersionsResponse();
        var ctx = new CapturingRouterContext(responseData);

        // When
        ctx.captureResult(router.onRequest(
                (short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(),
                responseData, ctx).toCompletableFuture().join());

        // Then
        assertThat(ctx.sentRequests())
                .as("API_VERSIONS should use anyNode when virtualNode is empty")
                .hasSize(1);
        assertThat(ctx.sentNodeRequests())
                .as("should not use specific node path")
                .isEmpty();
    }

    // --- PRODUCE: single route ---

    @Test
    void shouldRouteProduceToSingleCluster() {
        var request = produceRequest("orders.uk");
        var backendResp = produceResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, backendResp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(1);
        assertThat(ctx.sentNodeRequests().get(0).virtualNodeId()).isEqualTo(1);
        assertThat(ctx.sentResponseBody()).isInstanceOf(ProduceResponseData.class);
    }

    @Test
    void shouldFanOutProduceAcrossRoutes() {
        var request = produceRequest("orders.uk", "logs.app");
        var respA = produceResponse("orders.uk", 0, Errors.NONE);
        var respB = produceResponse("logs.app", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        primeLeaderCache(router, 2, "logs.app");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, respA, 2, respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(2);
        assertThat(ctx.sentNodeRequests()).extracting(SentNodeRequest::virtualNodeId)
                .containsExactlyInAnyOrder(1, 2);

        var merged = (ProduceResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).extracting("name")
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldRouteUnmatchedTopicToDefaultRoute() {
        var request = produceRequest("unknown.topic");
        var defaultResp = produceResponse("unknown.topic", 0, Errors.NONE);

        primeLeaderCache(router, 0, "unknown.topic");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(0, defaultResp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(1);
        assertThat(ctx.sentNodeRequests().get(0).virtualNodeId()).isEqualTo(0);
    }

    @Test
    void shouldSynthesiseErrorForUnroutableTopicsWithNoDefault() {
        var noDefaultTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = produceRequest("orders.uk", "logs.app");
        var respA = produceResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(noDefaultRouter, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, respA));
        ctx.captureResult(noDefaultRouter.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (ProduceResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).hasSize(2);
        var logsResp = findResponse(merged, "logs.app");
        assertThat(logsResp.partitionResponses().get(0).errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
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

    @Test
    void shouldHandleAcksZeroWithSingleRoute() {
        var request = produceRequest("orders.uk");
        request.setAcks((short) 0);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", new ProduceResponseData()));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentResponseBody()).isNotNull();
    }

    @Test
    void shouldHandleAcksZeroWithFanOut() {
        var request = produceRequest("orders.uk", "logs.app");
        request.setAcks((short) 0);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", new ProduceResponseData(), "cluster-b", new ProduceResponseData()));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentResponseBody()).isNotNull();
    }

    // --- INIT_PRODUCER_ID ---

    @Test
    void shouldFanOutInitProducerIdToAllRoutes() {
        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        var respA = initProducerIdResponse(100L, (short) 0);
        var respB = initProducerIdResponse(200L, (short) 0);
        var respDefault = initProducerIdResponse(300L, (short) 0);

        var ctx = new CapturingRouterContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respDefault));
        ctx.captureResult(router.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(3);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b", "default-route");
        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response.producerId()).isEqualTo(300L);
    }

    @Test
    void shouldReturnErrorIfAnyRouteFailsInitProducerId() {
        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        var respA = initProducerIdResponse(100L, (short) 0);
        var respFail = new InitProducerIdResponseData()
                .setErrorCode(Errors.COORDINATOR_NOT_AVAILABLE.code())
                .setProducerId(-1L)
                .setProducerEpoch((short) -1);
        var respDefault = initProducerIdResponse(300L, (short) 0);

        var ctx = new CapturingRouterContext(
                Map.of("cluster-a", respA, "cluster-b", respFail, "default-route", respDefault));
        ctx.captureResult(router.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response.errorCode()).isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE.code());
    }

    @Test
    void shouldPassThroughInitProducerIdWithSingleRoute() {
        var singleRouteTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "only-route"), null);
        var singleRouter = new TopicPartitionRouter(singleRouteTable, "only-route", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        var resp = initProducerIdResponse(42L, (short) 0);

        var ctx = new CapturingRouterContext(Map.of("only-route", resp));
        ctx.captureResult(singleRouter.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("only-route");
        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response.producerId()).isEqualTo(42L);
    }

    @Test
    void shouldRewriteInitProducerIdOnReinit() {
        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        var respA = initProducerIdResponse(100L, (short) 0);
        var respB = initProducerIdResponse(200L, (short) 0);
        var respDefault = initProducerIdResponse(300L, (short) 0);

        var ctx = new CapturingRouterContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respDefault));
        ctx.captureResult(router.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var reinitRequest = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000)
                .setProducerId(300L)
                .setProducerEpoch((short) 0);
        var reinitRespA = initProducerIdResponse(100L, (short) 1);
        var reinitRespB = initProducerIdResponse(200L, (short) 1);
        var reinitRespDefault = initProducerIdResponse(300L, (short) 1);

        var ctx2 = new CapturingRouterContext(
                Map.of("cluster-a", reinitRespA, "cluster-b", reinitRespB, "default-route", reinitRespDefault));
        ctx2.captureResult(router.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), reinitRequest, ctx2).toCompletableFuture().join());

        for (var sent : ctx2.sentRequests()) {
            var body = (InitProducerIdRequestData) sent.body();
            if (sent.route().equals("cluster-a")) {
                assertThat(body.producerId()).isEqualTo(100L);
            }
            else if (sent.route().equals("cluster-b")) {
                assertThat(body.producerId()).isEqualTo(200L);
            }
            else {
                assertThat(body.producerId()).isEqualTo(300L);
            }
        }
    }

    @Test
    void shouldRouteTransactionalInitProducerIdToSubjectRoute() {
        var txnRouter = createTxnRouter("cluster-a");
        var request = new InitProducerIdRequestData()
                .setTransactionalId("my-txn-id")
                .setTransactionTimeoutMs(60000);

        var initResp = initProducerIdResponse(100L, (short) 0);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", initResp))
                .withSubject(TXN_SUBJECT);
        ctx.captureResult(txnRouter.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response).isNotNull();
        assertThat(response.producerId()).isEqualTo(100L);
        txnRouter.close();
    }

    @Test
    void shouldRouteTransactionalInitProducerIdToDefaultRouteWhenUnmapped() {
        var txnRouter = createTxnRouter("cluster-a");
        var request = new InitProducerIdRequestData()
                .setTransactionalId("my-txn-id")
                .setTransactionTimeoutMs(60000);

        int coordinatorNodeId = 0;
        var findCoordResp = findCoordinatorResponse(coordinatorNodeId);
        var initResp = initProducerIdResponse(300L, (short) 0);

        testTopologyService.primeCoordinator("default-route", (byte) 1, "my-txn-id", new TestVirtualNode(coordinatorNodeId));
        var ctx = new SingleRouteInitCapturingContext("default-route", coordinatorNodeId,
                findCoordResp, initResp) {
            @Override
            public Subject authenticatedSubject() {
                return Subject.anonymous();
            }
        };
        ctx.captureResult(txnRouter.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response).isNotNull();
        assertThat(response.producerId()).isEqualTo(300L);
        txnRouter.close();
    }

    // --- idempotent produce ---

    @Test
    void shouldNotRewriteProducerIdForDefaultRoute() {
        setupProducerIdMapping(300L, Map.of(
                "default-route", new ProducerIdManager.ProducerIdEpoch(300L, (short) 0),
                "cluster-a", new ProducerIdManager.ProducerIdEpoch(100L, (short) 0),
                "cluster-b", new ProducerIdManager.ProducerIdEpoch(200L, (short) 0)));

        var request = idempotentProduceRequest(300L, (short) 0, "unknown.topic");
        var resp = produceResponse("unknown.topic", 0, Errors.NONE);

        primeLeaderCache(router, 0, "unknown.topic");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(0, resp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var sent = ctx.sentNodeRequests().get(0);
        var sentReq = (ProduceRequestData) sent.body();
        var records = (MemoryRecords) sentReq.topicData().iterator().next()
                .partitionData().iterator().next().records();
        var batch = records.batches().iterator().next();
        assertThat(batch.producerId()).isEqualTo(300L);
    }

    @Test
    void shouldRewriteProducerIdForNonDefaultRoute() {
        setupProducerIdMapping(300L, Map.of(
                "default-route", new ProducerIdManager.ProducerIdEpoch(300L, (short) 0),
                "cluster-a", new ProducerIdManager.ProducerIdEpoch(100L, (short) 0),
                "cluster-b", new ProducerIdManager.ProducerIdEpoch(200L, (short) 0)));

        var request = idempotentProduceRequest(300L, (short) 0, "orders.uk");
        var resp = produceResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, resp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var sent = ctx.sentNodeRequests().get(0);
        var sentReq = (ProduceRequestData) sent.body();
        var records = (MemoryRecords) sentReq.topicData().iterator().next()
                .partitionData().iterator().next().records();
        var batch = records.batches().iterator().next();
        assertThat(batch.producerId()).isEqualTo(100L);
    }

    @Test
    void shouldRewriteOnlyNonDefaultRoutesInFanOut() {
        setupProducerIdMapping(300L, Map.of(
                "default-route", new ProducerIdManager.ProducerIdEpoch(300L, (short) 0),
                "cluster-a", new ProducerIdManager.ProducerIdEpoch(100L, (short) 0),
                "cluster-b", new ProducerIdManager.ProducerIdEpoch(200L, (short) 0)));

        var request = idempotentProduceRequest(300L, (short) 0, "orders.uk", "logs.app");
        var respA = produceResponse("orders.uk", 0, Errors.NONE);
        var respB = produceResponse("logs.app", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        primeLeaderCache(router, 2, "logs.app");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, respA, 2, respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(2);
        for (var sent : ctx.sentNodeRequests()) {
            var sentReq = (ProduceRequestData) sent.body();
            var records = (MemoryRecords) sentReq.topicData().iterator().next()
                    .partitionData().iterator().next().records();
            var batch = records.batches().iterator().next();
            if (sent.virtualNodeId() == 1) {
                assertThat(batch.producerId()).isEqualTo(100L);
            }
            else if (sent.virtualNodeId() == 2) {
                assertThat(batch.producerId()).isEqualTo(200L);
            }
        }
    }

    @Test
    void shouldReturnUnknownProducerIdWhenMappingMissing() {
        var request = idempotentProduceRequest(999L, (short) 0, "orders.uk", "logs.app");

        var ctx = new CapturingRouterContext(Map.of());
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).as("no requests should be forwarded").isEmpty();
        var response = (ProduceResponseData) ctx.sentResponseBody();
        assertThat(response.responses()).hasSize(2);
        for (var tr : response.responses()) {
            for (var pr : tr.partitionResponses()) {
                assertThat(pr.errorCode()).isEqualTo(Errors.UNKNOWN_PRODUCER_ID.code());
            }
        }
    }

    private void setupProducerIdMapping(long clientProducerId,
                                        Map<String, ProducerIdManager.ProducerIdEpoch> mapping) {
        // Use INIT_PRODUCER_ID to establish mappings
        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        Map<String, ApiMessage> responses = new HashMap<>();
        for (var entry : mapping.entrySet()) {
            responses.put(entry.getKey(),
                    initProducerIdResponse(entry.getValue().producerId(), entry.getValue().producerEpoch()));
        }
        var ctx = new CapturingRouterContext(responses);
        ctx.captureResult(router.onRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx).toCompletableFuture().join());
    }

    // --- METADATA ---

    @Test
    void shouldRouteMetadataToSingleCluster() {
        var request = metadataRequest("orders.uk");
        var backendResp = metadataResponse(
                List.of(broker(0, "host-a", 9092)),
                List.of(topicMetadata("orders.uk", 0)));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        assertThat(ctx.sentResponseBody()).isInstanceOf(MetadataResponseData.class);
    }

    @Test
    void shouldFanOutMetadataAcrossRoutes() {
        var request = metadataRequest("orders.uk", "logs.app");
        var respA = metadataResponse(
                List.of(broker(0, "host-a", 9092)),
                List.of(topicMetadata("orders.uk", 0)));
        var respB = metadataResponse(
                List.of(broker(1, "host-b", 9092)),
                List.of(topicMetadata("logs.app", 1)));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", respA, "cluster-b", respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b");

        var merged = (MetadataResponseData) ctx.sentResponseBody();
        assertThat(merged.topics()).extracting(MetadataResponseTopic::name)
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldFanOutAllTopicsMetadataToAllRoutes() {
        var request = new MetadataRequestData().setTopics(null);
        var respA = metadataResponse(
                List.of(broker(0, "host-a", 9092)),
                List.of(topicMetadata("orders.uk", 0)));
        respA.setClusterId("cluster-a-id");
        var respB = metadataResponse(
                List.of(broker(1, "host-b", 9092)),
                List.of(topicMetadata("logs.app", 1)));

        var ctx = new CapturingRouterContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respA));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(ctx.sentResponseBody()).isInstanceOf(MetadataResponseData.class);
    }

    @Test
    void shouldFanOutBrokerOnlyMetadataToAllRoutes() {
        var request = new MetadataRequestData();
        var defaultResp = metadataResponse(
                List.of(broker(0, "host-default", 9092)),
                List.of());
        defaultResp.setClusterId("default-cluster");
        var respA = metadataResponse(
                List.of(broker(1, "host-a", 9092)),
                List.of());
        respA.setClusterId("cluster-a-id");
        var respB = metadataResponse(
                List.of(broker(2, "host-b", 9092)),
                List.of());
        respB.setClusterId("cluster-b-id");

        var ctx = new CapturingRouterContext(
                Map.of("default-route", defaultResp, "cluster-a", respA, "cluster-b", respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(3);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("default-route", "cluster-a", "cluster-b");
    }

    @Test
    void shouldMergeBrokerListsInMetadataResponse() {
        var request = metadataRequest("orders.uk", "logs.app");
        var respA = metadataResponse(
                List.of(broker(0, "host-a", 9092), broker(1, "host-a", 9093)),
                List.of(topicMetadata("orders.uk", 0)));
        respA.setClusterId("cluster-a-id");
        var respB = metadataResponse(
                List.of(broker(2, "host-b", 9092)),
                List.of(topicMetadata("logs.app", 2)));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", respA, "cluster-b", respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (MetadataResponseData) ctx.sentResponseBody();
        assertThat(merged.brokers()).hasSize(3);
        assertThat(merged.brokers()).extracting(MetadataResponseBroker::nodeId)
                .containsExactlyInAnyOrder(0, 1, 2);
    }

    // --- FETCH ---

    @Test
    void shouldRouteFetchToSingleCluster() {
        var request = fetchRequest("orders.uk");
        request.setSessionId(0);
        request.setSessionEpoch(-1);
        var backendResp = fetchResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, backendResp));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(1);
        var sentReq = (FetchRequestData) ctx.sentNodeRequests().get(0).body();
        assertThat(sentReq.sessionId()).as("no client session → backend gets session creation").isEqualTo(0);
        assertThat(sentReq.topics()).extracting("topic").containsExactly("orders.uk");
    }

    @Test
    void shouldFanOutFetchAcrossRoutes() {
        var request = fetchRequest("orders.uk", "logs.app");
        request.setSessionId(0);
        request.setSessionEpoch(-1);
        var respA = fetchResponse("orders.uk", 0, Errors.NONE);
        var respB = fetchResponse("logs.app", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        primeLeaderCache(router, 2, "logs.app");
        var ctx = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, respA, 2, respB));
        ctx.captureResult(router.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(2);
        assertThat(ctx.sentNodeRequests()).extracting(SentNodeRequest::virtualNodeId)
                .containsExactlyInAnyOrder(1, 2);

        for (var sent : ctx.sentNodeRequests()) {
            var sentReq = (FetchRequestData) sent.body();
            if (sent.virtualNodeId() == 1) {
                assertThat(sentReq.topics()).extracting("topic").containsExactly("orders.uk");
            }
            else {
                assertThat(sentReq.topics()).extracting("topic").containsExactly("logs.app");
            }
        }

        var merged = (FetchResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).extracting("topic")
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldSynthesiseErrorForUnroutableFetchTopics() {
        var noDefaultTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = fetchRequest("orders.uk", "unknown.topic");
        request.setSessionId(0);
        request.setSessionEpoch(-1);
        var respA = fetchResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(noDefaultRouter, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, respA));
        ctx.captureResult(noDefaultRouter.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (FetchResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).hasSize(2);
        var unknownResp = merged.responses().stream()
                .filter(t -> t.topic().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(unknownResp.partitions().get(0).errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    @Test
    void shouldCreatePerLeaderFetchSessions() {
        // Two topics on the same route but different leaders should get separate sessions
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), "default-route");
        var sessionRouter = new TopicPartitionRouter(table, "default-route", Map.of(),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 10_000, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        primeLeaderCache(sessionRouter, 1, "orders.uk");
        primeLeaderCache(sessionRouter, 2, "orders.de");

        // First fetch: backend returns sessionId to establish the session
        var request1 = fetchRequest("orders.uk", "orders.de");
        request1.setSessionId(0);
        request1.setSessionEpoch(0);
        var resp1 = fetchResponse("orders.uk", 0, Errors.NONE);
        resp1.setSessionId(100);
        var resp2 = fetchResponse("orders.de", 0, Errors.NONE);
        resp2.setSessionId(200);

        var ctx1 = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, resp1, 2, resp2));
        ctx1.captureResult(sessionRouter.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request1, ctx1).toCompletableFuture().join());

        assertThat(ctx1.sentNodeRequests()).hasSize(2);
        for (var sent : ctx1.sentNodeRequests()) {
            var sentReq = (FetchRequestData) sent.body();
            assertThat(sentReq.sessionEpoch())
                    .as("leader %d should get session creation", sent.virtualNodeId())
                    .isEqualTo(0);
        }

        // Second fetch: each leader should get incremental request with its own session ID
        var request2 = fetchRequest("orders.uk", "orders.de");
        request2.setSessionId(0);
        request2.setSessionEpoch(-1);
        var ctx2 = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, resp1, 2, resp2));
        ctx2.captureResult(sessionRouter.onRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request2, ctx2).toCompletableFuture().join());

        assertThat(ctx2.sentNodeRequests()).hasSize(2);
        for (var sent : ctx2.sentNodeRequests()) {
            var sentReq = (FetchRequestData) sent.body();
            if (sent.virtualNodeId() == 1) {
                assertThat(sentReq.sessionId()).as("leader 1 session").isEqualTo(100);
            }
            else {
                assertThat(sentReq.sessionId()).as("leader 2 session").isEqualTo(200);
            }
        }
    }

    // --- LIST_OFFSETS ---

    @Test
    void shouldRouteListOffsetsToSingleCluster() {
        var request = listOffsetsRequest("orders.uk");
        var backendResp = listOffsetsResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(router, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, backendResp));
        ctx.captureResult(router.onRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(1);
        var sentReq = (ListOffsetsRequestData) ctx.sentNodeRequests().get(0).body();
        assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
    }

    @Test
    void shouldFanOutListOffsetsAcrossRoutes() {
        var request = listOffsetsRequest("orders.uk", "logs.app");
        var respA = listOffsetsResponse("orders.uk", 0, Errors.NONE);
        var respB = listOffsetsResponse("logs.app", 0, Errors.NONE);

        var ctx = new CapturingRouterContext(Map.of())
                .withDefaultMetadataResponse(metadataResponseWithLeaders(1, "orders.uk"))
                .withNodeResponses(Map.of(1, respA, 2, respB));
        // Prime leader cache for both routes
        primeLeaderCache(router, 1, "orders.uk");
        primeLeaderCache(router, 2, "logs.app");
        ctx.captureResult(router.onRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(2);
        assertThat(ctx.sentNodeRequests()).extracting(SentNodeRequest::virtualNodeId)
                .containsExactlyInAnyOrder(1, 2);

        for (var sent : ctx.sentNodeRequests()) {
            var sentReq = (ListOffsetsRequestData) sent.body();
            if (sent.virtualNodeId() == 1) {
                assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
            }
            else {
                assertThat(sentReq.topics()).extracting("name").containsExactly("logs.app");
            }
        }

        var merged = (ListOffsetsResponseData) ctx.sentResponseBody();
        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldSynthesiseErrorForUnroutableListOffsetsTopics() {
        var noDefaultTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = listOffsetsRequest("orders.uk", "unknown.topic");
        var respA = listOffsetsResponse("orders.uk", 0, Errors.NONE);

        primeLeaderCache(noDefaultRouter, 1, "orders.uk");
        var ctx = new CapturingRouterContext(Map.of())
                .withNodeResponses(Map.of(1, respA));
        ctx.captureResult(noDefaultRouter.onRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (ListOffsetsResponseData) ctx.sentResponseBody();
        assertThat(merged.topics()).hasSize(2);
        var unknownResp = merged.topics().stream()
                .filter(t -> t.name().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(unknownResp.partitions().get(0).errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    // --- OFFSET_COMMIT ---

    @Test
    void shouldRouteOffsetCommitToSingleCluster() {
        var request = offsetCommitRequest("orders.uk");
        var backendResp = offsetCommitResponse("orders.uk", 0, Errors.NONE);

        testTopologyService.primeCoordinator("cluster-a", (byte) 0, "test-group", new TestVirtualNode(1));
        var ctx = new CapturingRouterContext(Map.of())
                .withFindCoordinatorNodeIds(Map.of("cluster-a", 1))
                .withNodeResponses(Map.of(1, backendResp));
        ctx.captureResult(router.onRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(1);
        assertThat(ctx.sentNodeRequests().get(0).virtualNodeId()).isEqualTo(1);

        var sentReq = (OffsetCommitRequestData) ctx.sentNodeRequests().get(0).body();
        assertThat(sentReq.groupId()).isEqualTo("test-group");
        assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
    }

    @Test
    void shouldFanOutOffsetCommitAcrossRoutes() {
        var request = offsetCommitRequest("orders.uk", "logs.app");
        var respA = offsetCommitResponse("orders.uk", 0, Errors.NONE);
        var respB = offsetCommitResponse("logs.app", 0, Errors.NONE);

        testTopologyService.primeCoordinator("cluster-a", (byte) 0, "test-group", new TestVirtualNode(1));
        testTopologyService.primeCoordinator("cluster-b", (byte) 0, "test-group", new TestVirtualNode(2));
        var ctx = new CapturingRouterContext(Map.of())
                .withFindCoordinatorNodeIds(Map.of("cluster-a", 1, "cluster-b", 2))
                .withNodeResponses(Map.of(1, respA, 2, respB));
        ctx.captureResult(router.onRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentNodeRequests()).hasSize(2);
        assertThat(ctx.sentNodeRequests()).extracting(SentNodeRequest::virtualNodeId)
                .containsExactlyInAnyOrder(1, 2);

        for (var sent : ctx.sentNodeRequests()) {
            var sentReq = (OffsetCommitRequestData) sent.body();
            assertThat(sentReq.groupId()).as("groupId preserved on node %d", sent.virtualNodeId())
                    .isEqualTo("test-group");
            if (sent.virtualNodeId() == 1) {
                assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
            }
            else {
                assertThat(sentReq.topics()).extracting("name").containsExactly("logs.app");
            }
        }

        var merged = (OffsetCommitResponseData) ctx.sentResponseBody();
        assertThat(merged.topics()).extracting("name")
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldSynthesiseErrorForUnroutableOffsetCommitTopics() {
        var noDefaultTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", Map.of(), new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = offsetCommitRequest("orders.uk", "unknown.topic");
        var respA = offsetCommitResponse("orders.uk", 0, Errors.NONE);

        testTopologyService.primeCoordinator("cluster-a", (byte) 0, "test-group", new TestVirtualNode(1));
        var ctx = new CapturingRouterContext(Map.of())
                .withFindCoordinatorNodeIds(Map.of("cluster-a", 1))
                .withNodeResponses(Map.of(1, respA));
        ctx.captureResult(noDefaultRouter.onRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (OffsetCommitResponseData) ctx.sentResponseBody();
        assertThat(merged.topics()).hasSize(2);
        var unknownResp = merged.topics().stream()
                .filter(t -> t.name().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(unknownResp.partitions().get(0).errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    // --- FIND_COORDINATOR ---

    @Test
    void shouldForwardFindCoordinatorToDefaultRoute() {
        var request = new FindCoordinatorRequestData()
                .setKey("test-group")
                .setKeyType((byte) 0);

        var backendResp = new FindCoordinatorResponseData()
                .setNodeId(1)
                .setHost("broker-1")
                .setPort(9092);

        var ctx = new CapturingRouterContext(Map.of("default-route", backendResp));
        ctx.captureResult(router.onRequest(
                (short) 3, ApiKeys.FIND_COORDINATOR, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
        var resp = (FindCoordinatorResponseData) ctx.sentResponseBody();
        assertThat(resp.nodeId()).isEqualTo(1);
    }

    @Test
    void shouldRouteFindCoordinatorToMappedRouteForTransactionKeyType() {
        var txnRouter = createTxnRouter("cluster-a");
        var request = new FindCoordinatorRequestData()
                .setKey("my-txn-id")
                .setKeyType((byte) 1);

        var backendResp = new FindCoordinatorResponseData()
                .setNodeId(1)
                .setHost("broker-1")
                .setPort(9092);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp))
                .withSubject(TXN_SUBJECT);
        ctx.captureResult(txnRouter.onRequest(
                (short) 3, ApiKeys.FIND_COORDINATOR, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        txnRouter.close();
    }

    @Test
    void shouldRouteFindCoordinatorToSubjectRouteForGroupKeyType() {
        var txnRouter = createTxnRouter("cluster-a");
        var request = new FindCoordinatorRequestData()
                .setKey("my-group")
                .setKeyType((byte) 0);

        var backendResp = new FindCoordinatorResponseData()
                .setNodeId(0)
                .setHost("broker-0")
                .setPort(9092);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp))
                .withSubject(TXN_SUBJECT);
        ctx.captureResult(txnRouter.onRequest(
                (short) 3, ApiKeys.FIND_COORDINATOR, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        txnRouter.close();
    }

    @Test
    void shouldRouteFindCoordinatorToDefaultRouteForUnmappedUser() {
        var txnRouter = createTxnRouter("cluster-a");
        var request = new FindCoordinatorRequestData()
                .setKey("my-txn-id")
                .setKeyType((byte) 1);

        var backendResp = new FindCoordinatorResponseData()
                .setNodeId(0)
                .setHost("broker-0")
                .setPort(9092);

        var ctx = new CapturingRouterContext(Map.of("default-route", backendResp));
        ctx.captureResult(txnRouter.onRequest(
                (short) 3, ApiKeys.FIND_COORDINATOR, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
        txnRouter.close();
    }

    // --- DESCRIBE_CLUSTER ---

    @Test
    void shouldFanOutDescribeClusterAndMergeBrokers() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "cluster-a");
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a", Map.of(),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = new DescribeClusterRequestData()
                .setIncludeClusterAuthorizedOperations(false);

        var respA = describeClusterResponse("cluster-A", 0, 100);
        respA.brokers().add(describeClusterBroker(0, "hostA-0", 9092));
        respA.brokers().add(describeClusterBroker(1, "hostA-1", 9093));

        var respB = describeClusterResponse("cluster-B", 1, 200);
        respB.brokers().add(describeClusterBroker(2, "hostB-0", 9092));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", respA, "cluster-b", respB));
        ctx.captureResult(twoRouteRouter.onRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (DescribeClusterResponseData) ctx.sentResponseBody();
        assertThat(merged.brokers()).hasSize(3);
        assertThat(merged.brokers().find(0)).isNotNull();
        assertThat(merged.brokers().find(1)).isNotNull();
        assertThat(merged.brokers().find(2)).isNotNull();

        twoRouteRouter.close();
    }

    @Test
    void shouldUseDefaultRouteClusterFieldsInDescribeCluster() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "cluster-a");
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a", Map.of(),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = new DescribeClusterRequestData();

        var respDefault = describeClusterResponse("default-cluster", 42, 100);
        respDefault.setClusterAuthorizedOperations(0xFF);
        respDefault.brokers().add(describeClusterBroker(0, "host0", 9092));

        var respOther = describeClusterResponse("other-cluster", 99, 50);
        respOther.setClusterAuthorizedOperations(0x01);
        respOther.brokers().add(describeClusterBroker(1, "host1", 9092));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", respDefault, "cluster-b", respOther));
        ctx.captureResult(twoRouteRouter.onRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (DescribeClusterResponseData) ctx.sentResponseBody();
        assertThat(merged.clusterId()).isEqualTo("default-cluster");
        assertThat(merged.controllerId()).isEqualTo(42);
        assertThat(merged.clusterAuthorizedOperations()).isEqualTo(0xFF);

        twoRouteRouter.close();
    }

    @Test
    void shouldTakeMaxThrottleTimeInDescribeCluster() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "cluster-a");
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a", Map.of(),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);

        var request = new DescribeClusterRequestData();

        var respA = describeClusterResponse("c1", 0, 100);
        respA.brokers().add(describeClusterBroker(0, "h0", 9092));
        var respB = describeClusterResponse("c2", 1, 300);
        respB.brokers().add(describeClusterBroker(1, "h1", 9092));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", respA, "cluster-b", respB));
        ctx.captureResult(twoRouteRouter.onRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        var merged = (DescribeClusterResponseData) ctx.sentResponseBody();
        assertThat(merged.throttleTimeMs()).isEqualTo(300);

        twoRouteRouter.close();
    }

    // --- ADD_PARTITIONS_TO_TXN ---

    @Test
    void shouldRouteAddPartitionsToTxnToSubjectRoute() {
        var txnRouter = createTxnRouter("cluster-a");

        var request = addPartitionsToTxnRequest("my-txn-id", 100L, (short) 0,
                "orders.uk");
        var backendResp = addPartitionsToTxnResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp))
                .withSubject(TXN_SUBJECT);
        ctx.captureResult(txnRouter.onRequest((short) 3, ApiKeys.ADD_PARTITIONS_TO_TXN,
                new RequestHeaderData(), request, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        assertThat(ctx.sentResponseBody())
                .isInstanceOf(AddPartitionsToTxnResponseData.class);
        txnRouter.close();
    }

    // --- END_TXN ---

    @Test
    void shouldRouteEndTxnToSubjectRoute() {
        var txnRouter = createTxnRouter("cluster-a");

        var endReq = new EndTxnRequestData()
                .setTransactionalId("my-txn-id")
                .setProducerId(100L)
                .setProducerEpoch((short) 0)
                .setCommitted(true);
        var endResp = new EndTxnResponseData().setErrorCode(Errors.NONE.code());

        var endCtx = new CapturingRouterContext(Map.of("cluster-a", endResp))
                .withSubject(TXN_SUBJECT);
        endCtx.captureResult(txnRouter.onRequest((short) 3, ApiKeys.END_TXN,
                new RequestHeaderData(), endReq, endCtx).toCompletableFuture().join());

        assertThat(endCtx.sentRequests()).hasSize(1);
        assertThat(endCtx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        assertThat(endCtx.sentResponseBody())
                .isInstanceOf(EndTxnResponseData.class);
        txnRouter.close();
    }

    @Test
    void shouldRouteEndTxnToDefaultRouteForUnmappedUser() {
        var endReq = new EndTxnRequestData()
                .setTransactionalId("my-txn-id")
                .setProducerId(300L)
                .setProducerEpoch((short) 0)
                .setCommitted(true);
        var endResp = new EndTxnResponseData().setErrorCode(Errors.NONE.code());

        var ctx = new CapturingRouterContext(Map.of("default-route", endResp));
        ctx.captureResult(router.onRequest((short) 3, ApiKeys.END_TXN,
                new RequestHeaderData(), endReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
    }

    @Test
    void shouldForwardEndTxnUnmodifiedForSubjectRoutedUser() {
        var txnRouter = createTxnRouter("cluster-b");

        var endReq = new EndTxnRequestData()
                .setTransactionalId("my-txn-id")
                .setProducerId(200L)
                .setProducerEpoch((short) 0)
                .setCommitted(true);
        var endResp = new EndTxnResponseData().setErrorCode(Errors.NONE.code());
        var endCtx = new CapturingRouterContext(Map.of("cluster-b", endResp))
                .withSubject(TXN_SUBJECT);
        endCtx.captureResult(txnRouter.onRequest((short) 3, ApiKeys.END_TXN,
                new RequestHeaderData(), endReq, endCtx).toCompletableFuture().join());

        assertThat(endCtx.sentRequests()).hasSize(1);
        assertThat(endCtx.sentRequests().get(0).route()).isEqualTo("cluster-b");
        var sentReq = (EndTxnRequestData) endCtx.sentRequests().get(0).body();
        assertThat(sentReq.producerId()).isEqualTo(200L);
        txnRouter.close();
    }

    // --- CONSUMER_GROUP_HEARTBEAT ---

    @Test
    void shouldRouteConsumerGroupHeartbeatToSubjectRoute() {
        var cgRouter = createCgRouter("cluster-b");
        var heartbeatReq = new ConsumerGroupHeartbeatRequestData()
                .setGroupId("my-group")
                .setMemberId("member-1")
                .setMemberEpoch(0);

        var heartbeatResp = new ConsumerGroupHeartbeatResponseData()
                .setMemberId("member-1")
                .setMemberEpoch(1)
                .setHeartbeatIntervalMs(5000);

        var ctx = new CapturingRouterContext(Map.of("cluster-b", heartbeatResp))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 0, ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                new RequestHeaderData(), heartbeatReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-b");
        assertThat(ctx.sentResponseBody()).isNotNull();
        var respBody = (ConsumerGroupHeartbeatResponseData) ctx.sentResponseBody();
        assertThat(respBody.memberId()).isEqualTo("member-1");
        cgRouter.close();
    }

    @Test
    void shouldRouteConsumerGroupHeartbeatToDefaultRouteWhenUnmapped() {
        var cgRouter = createCgRouter("cluster-b");
        var heartbeatReq = new ConsumerGroupHeartbeatRequestData()
                .setGroupId("my-group")
                .setMemberId("member-1")
                .setMemberEpoch(0);

        var heartbeatResp = new ConsumerGroupHeartbeatResponseData()
                .setMemberId("member-1")
                .setMemberEpoch(1);

        var ctx = new SingleRouteInitCapturingContext(
                "default-route", heartbeatResp, cgRouter);
        ctx.withSubject(new Subject(new User("unmapped-user")));

        ctx.captureResult(cgRouter.onRequest((short) 0, ApiKeys.CONSUMER_GROUP_HEARTBEAT,
                new RequestHeaderData(), heartbeatReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentResponseBody()).isNotNull();
        cgRouter.close();
    }

    // --- CONSUMER_GROUP_DESCRIBE ---

    @Test
    void shouldRouteConsumerGroupDescribeToMappedRoute() {
        var cgRouter = createCgRouter("cluster-b");
        var describeReq = new ConsumerGroupDescribeRequestData();
        describeReq.groupIds().add("my-group");

        var ctx = new CapturingRouterContext(
                Map.of("cluster-b", new org.apache.kafka.common.message.ConsumerGroupDescribeResponseData()))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 0, ApiKeys.CONSUMER_GROUP_DESCRIBE,
                new RequestHeaderData(), describeReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-b");
        cgRouter.close();
    }

    // --- FIND_COORDINATOR for consumer groups ---

    @Test
    void shouldRouteFindCoordinatorToMappedRouteForConsumerGroupKeyType() {
        var cgRouter = createCgRouter("cluster-b");
        var findCoordReq = new FindCoordinatorRequestData()
                .setKey("my-group")
                .setKeyType((byte) 0);

        var ctx = new CapturingRouterContext(
                Map.of("cluster-b", new FindCoordinatorResponseData()))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 3, ApiKeys.FIND_COORDINATOR,
                new RequestHeaderData(), findCoordReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-b");
        cgRouter.close();
    }

    @Test
    void shouldRouteFindCoordinatorToDefaultRouteForUnmappedUserGroupKeyType() {
        var cgRouter = createCgRouter("cluster-b");
        var findCoordReq = new FindCoordinatorRequestData()
                .setKey("my-group")
                .setKeyType((byte) 0);

        var ctx = new CapturingRouterContext(
                Map.of("default-route", new FindCoordinatorResponseData()))
                .withSubject(new Subject(new User("unmapped-user")));

        ctx.captureResult(cgRouter.onRequest((short) 3, ApiKeys.FIND_COORDINATOR,
                new RequestHeaderData(), findCoordReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
        cgRouter.close();
    }

    // --- OFFSET_COMMIT with consumer group router ---

    @Test
    void shouldRouteOffsetCommitToGroupRouteWhenMapped() {
        var cgRouter = createCgRouter("cluster-a");

        var commitReq = new OffsetCommitRequestData()
                .setGroupId("my-group")
                .setMemberId("member-1")
                .setGenerationIdOrMemberEpoch(1);
        commitReq.topics().add(new OffsetCommitRequestTopic()
                .setName("orders.uk")
                .setPartitions(List.of(
                        new OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(100))));

        var backendResp = new OffsetCommitResponseData();
        backendResp.topics().add(new OffsetCommitResponseTopic()
                .setName("orders.uk")
                .setPartitions(List.of(
                        new OffsetCommitResponsePartition()
                                .setPartitionIndex(0)
                                .setErrorCode(Errors.NONE.code()))));

        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 9, ApiKeys.OFFSET_COMMIT,
                new RequestHeaderData(), commitReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        cgRouter.close();
    }

    @Test
    void shouldForwardOffsetCommitToSubjectRouteForCrossRouteTopics() {
        var cgRouter = createCgRouter("cluster-a");

        var commitReq = new OffsetCommitRequestData()
                .setGroupId("my-group")
                .setMemberId("member-1")
                .setGenerationIdOrMemberEpoch(1);
        commitReq.topics().add(new OffsetCommitRequestTopic()
                .setName("logs.errors")
                .setPartitions(List.of(
                        new OffsetCommitRequestPartition()
                                .setPartitionIndex(0)
                                .setCommittedOffset(50))));

        var backendResp = new OffsetCommitResponseData();
        var ctx = new CapturingRouterContext(Map.of("cluster-a", backendResp))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 9, ApiKeys.OFFSET_COMMIT,
                new RequestHeaderData(), commitReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        cgRouter.close();
    }

    // --- OFFSET_FETCH ---

    @Test
    void shouldRouteOffsetFetchToGroupRouteWhenMapped() {
        var cgRouter = createCgRouter("cluster-b");

        var fetchReq = new org.apache.kafka.common.message.OffsetFetchRequestData();
        fetchReq.setGroupId("my-group");
        fetchReq.topics().add(new org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic()
                .setName("logs.errors")
                .setPartitionIndexes(List.of(0)));

        var backendResp = new org.apache.kafka.common.message.OffsetFetchResponseData();

        var ctx = new CapturingRouterContext(Map.of("cluster-b", backendResp))
                .withSubject(CG_SUBJECT);

        ctx.captureResult(cgRouter.onRequest((short) 7, ApiKeys.OFFSET_FETCH,
                new RequestHeaderData(), fetchReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-b");
        cgRouter.close();
    }

    @Test
    void shouldDecomposeOffsetFetchByTopicWhenUnmapped() {
        var fetchReq = new org.apache.kafka.common.message.OffsetFetchRequestData();
        fetchReq.setGroupId("my-group");
        fetchReq.topics().add(new org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic()
                .setName("orders.uk")
                .setPartitionIndexes(List.of(0)));
        fetchReq.topics().add(new org.apache.kafka.common.message.OffsetFetchRequestData.OffsetFetchRequestTopic()
                .setName("logs.errors")
                .setPartitionIndexes(List.of(0)));

        var respA = new org.apache.kafka.common.message.OffsetFetchResponseData();
        respA.topics().add(new org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic()
                .setName("orders.uk"));
        var respB = new org.apache.kafka.common.message.OffsetFetchResponseData();
        respB.topics().add(new org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic()
                .setName("logs.errors"));

        testTopologyService.primeCoordinator("cluster-a", (byte) 0, "my-group", new TestVirtualNode(1));
        testTopologyService.primeCoordinator("cluster-b", (byte) 0, "my-group", new TestVirtualNode(2));
        var ctx = new CapturingRouterContext(Map.of())
                .withFindCoordinatorNodeIds(Map.of("cluster-a", 1, "cluster-b", 2))
                .withNodeResponses(Map.of(1, respA, 2, respB));

        ctx.captureResult(router.onRequest((short) 7, ApiKeys.OFFSET_FETCH,
                new RequestHeaderData(), fetchReq, ctx).toCompletableFuture().join());

        assertThat(ctx.sentResponseBody).isNotNull();
        var resp = (org.apache.kafka.common.message.OffsetFetchResponseData) ctx.sentResponseBody;
        assertThat(resp.topics()).hasSize(2);
    }

    // --- consumer group helpers ---

    private static final Subject CG_SUBJECT = new Subject(new User("cg-user"));

    private TopicPartitionRouter createCgRouter(String mappedRoute) {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "default-route");
        return new TopicPartitionRouter(table, "default-route",
                Map.of("cg-user", mappedRoute),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);
    }

    // --- transaction helpers ---

    private static final Subject TXN_SUBJECT = new Subject(new User("txn-user"));

    private TopicPartitionRouter createTxnRouter(String mappedRoute) {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "default-route");
        return new TopicPartitionRouter(table, "default-route",
                Map.of("txn-user", mappedRoute),
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter", testTopologyService);
    }

    private void setupTransactionalProducer(TopicPartitionRouter txnRouter,
                                            String transactionalId,
                                            String route,
                                            long producerId) {
        var initReq = new InitProducerIdRequestData()
                .setTransactionalId(transactionalId)
                .setTransactionTimeoutMs(60000);

        int coordinatorNodeId = route.equals("cluster-a") ? 1 : 2;
        var findCoordResp = findCoordinatorResponse(coordinatorNodeId);
        var initResp = initProducerIdResponse(producerId, (short) 0);

        var ctx = new SingleRouteInitCapturingContext(route, coordinatorNodeId,
                findCoordResp, initResp);
        ctx.captureResult(txnRouter.onRequest((short) 5, ApiKeys.INIT_PRODUCER_ID,
                new RequestHeaderData(), initReq, ctx).toCompletableFuture().join());
    }

    private static FindCoordinatorResponseData findCoordinatorResponse(int nodeId) {
        return new FindCoordinatorResponseData()
                .setErrorCode(Errors.NONE.code())
                .setNodeId(nodeId)
                .setHost("localhost")
                .setPort(9092);
    }

    private static AddPartitionsToTxnRequestData addPartitionsToTxnRequest(
                                                                           String transactionalId,
                                                                           long producerId,
                                                                           short producerEpoch,
                                                                           String... topicNames) {
        var request = new AddPartitionsToTxnRequestData()
                .setV3AndBelowTransactionalId(transactionalId)
                .setV3AndBelowProducerId(producerId)
                .setV3AndBelowProducerEpoch(producerEpoch);
        for (String name : topicNames) {
            var topic = new AddPartitionsToTxnRequestData.AddPartitionsToTxnTopic()
                    .setName(name);
            topic.partitions().add(0);
            request.v3AndBelowTopics().add(topic);
        }
        return request;
    }

    private static AddPartitionsToTxnResponseData addPartitionsToTxnResponse(
                                                                             String topicName,
                                                                             int partition,
                                                                             Errors error) {
        var response = new AddPartitionsToTxnResponseData();
        var topicResult = new AddPartitionsToTxnResponseData.AddPartitionsToTxnTopicResult()
                .setName(topicName);
        topicResult.resultsByPartition().add(
                new AddPartitionsToTxnResponseData.AddPartitionsToTxnPartitionResult()
                        .setPartitionIndex(partition)
                        .setPartitionErrorCode(error.code()));
        response.resultsByTopicV3AndBelow().add(topicResult);
        return response;
    }

    // --- helpers ---

    private static DescribeClusterResponseData describeClusterResponse(String clusterId,
                                                                       int controllerId,
                                                                       int throttleTimeMs) {
        return new DescribeClusterResponseData()
                .setClusterId(clusterId)
                .setControllerId(controllerId)
                .setThrottleTimeMs(throttleTimeMs);
    }

    private static DescribeClusterResponseData.DescribeClusterBroker describeClusterBroker(
                                                                                           int brokerId,
                                                                                           String host,
                                                                                           int port) {
        return new DescribeClusterResponseData.DescribeClusterBroker()
                .setBrokerId(brokerId)
                .setHost(host)
                .setPort(port);
    }

    private static InitProducerIdResponseData initProducerIdResponse(long producerId,
                                                                     short producerEpoch) {
        return new InitProducerIdResponseData()
                .setProducerId(producerId)
                .setProducerEpoch(producerEpoch)
                .setErrorCode(Errors.NONE.code());
    }

    private static ProduceRequestData idempotentProduceRequest(long producerId,
                                                               short producerEpoch,
                                                               String... topicNames) {
        var request = new ProduceRequestData();
        request.setAcks((short) -1);
        request.setTimeoutMs(30000);
        for (var name : topicNames) {
            var td = new TopicProduceData().setName(name);
            td.partitionData().add(new PartitionProduceData()
                    .setIndex(0)
                    .setRecords(buildIdempotentRecords(producerId, producerEpoch, 0)));
            request.topicData().add(td);
        }
        return request;
    }

    private static MemoryRecords buildIdempotentRecords(long producerId,
                                                        short producerEpoch,
                                                        int baseSequence) {
        var buf = ByteBuffer.allocate(1024);
        var builder = new MemoryRecordsBuilder(
                new ByteBufferOutputStream(buf),
                RecordBatch.CURRENT_MAGIC_VALUE,
                Compression.NONE,
                TimestampType.CREATE_TIME,
                0L,
                RecordBatch.NO_TIMESTAMP,
                producerId,
                producerEpoch,
                baseSequence,
                false,
                false,
                RecordBatch.NO_PARTITION_LEADER_EPOCH,
                1024);
        builder.append(new SimpleRecord(
                System.currentTimeMillis(),
                "key".getBytes(StandardCharsets.UTF_8),
                "value".getBytes(StandardCharsets.UTF_8)));
        return builder.build();
    }

    private static ApiVersionsResponseData apiVersionsResponse() {
        var data = new ApiVersionsResponseData();
        for (ApiKeys key : ApiKeys.values()) {
            data.apiKeys().add(new ApiVersionsResponseData.ApiVersion()
                    .setApiKey(key.id)
                    .setMinVersion(key.oldestVersion())
                    .setMaxVersion(key.latestVersion()));
        }
        return data;
    }

    private static void setMaxVersion(ApiVersionsResponseData data,
                                      ApiKeys key,
                                      short maxVersion) {
        var version = data.apiKeys().find(key.id);
        if (version != null) {
            version.setMaxVersion(maxVersion);
        }
    }

    private static short findMaxVersion(ApiVersionsResponseData data,
                                        ApiKeys key) {
        var version = data.apiKeys().find(key.id);
        assertThat(version).isNotNull();
        return version.maxVersion();
    }

    private static ProduceRequestData produceRequest(String... topicNames) {
        var request = new ProduceRequestData();
        request.setAcks((short) -1);
        request.setTimeoutMs(30000);
        for (var name : topicNames) {
            var td = new TopicProduceData().setName(name);
            td.partitionData().add(new PartitionProduceData().setIndex(0));
            request.topicData().add(td);
        }
        return request;
    }

    private static ProduceResponseData produceResponse(String topicName,
                                                       int partition,
                                                       Errors error) {
        var resp = new ProduceResponseData();
        var tr = new TopicProduceResponse().setName(topicName);
        tr.partitionResponses().add(
                new PartitionProduceResponse()
                        .setIndex(partition)
                        .setErrorCode(error.code()));
        resp.responses().add(tr);
        return resp;
    }

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

    private static MetadataResponseData emptyMetadataResponse() {
        return metadataResponse(List.of(), List.of());
    }

    private void primeLeaderCache(TopicPartitionRouter router,
                                  int nodeId,
                                  String... topicNames) {
        for (String topicName : topicNames) {
            testTopologyService.primeLeader(topicName, 0, new TestVirtualNode(nodeId));
        }
    }

    /**
     * Creates a metadata response with a single broker and leader assignments
     * for the given topics (partition 0 → nodeId).
     */
    private static MetadataResponseData metadataResponseWithLeaders(int nodeId,
                                                                    String... topicNames) {
        var broker = new MetadataResponseBroker()
                .setNodeId(nodeId).setHost("localhost").setPort(9092);
        var topics = new ArrayList<MetadataResponseTopic>();
        for (var name : topicNames) {
            var topic = new MetadataResponseTopic()
                    .setName(name)
                    .setErrorCode(Errors.NONE.code());
            topic.partitions().add(
                    new MetadataResponsePartition()
                            .setPartitionIndex(0)
                            .setLeaderId(nodeId)
                            .setErrorCode(Errors.NONE.code()));
            topics.add(topic);
        }
        return metadataResponse(List.of(broker), topics);
    }

    private static FetchRequestData fetchRequest(String... topicNames) {
        var request = new FetchRequestData();
        for (var name : topicNames) {
            var topic = new FetchTopic().setTopic(name);
            topic.partitions().add(new FetchPartition().setPartition(0).setFetchOffset(0));
            request.topics().add(topic);
        }
        return request;
    }

    private static FetchResponseData fetchResponse(String topicName,
                                                   int partition,
                                                   Errors error) {
        var resp = new FetchResponseData();
        var tr = new FetchableTopicResponse().setTopic(topicName);
        tr.partitions().add(
                new PartitionData()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        resp.responses().add(tr);
        return resp;
    }

    private static ListOffsetsRequestData listOffsetsRequest(String... topicNames) {
        var request = new ListOffsetsRequestData();
        for (var name : topicNames) {
            var topic = new ListOffsetsTopic().setName(name);
            topic.partitions().add(new ListOffsetsPartition().setPartitionIndex(0).setTimestamp(-1));
            request.topics().add(topic);
        }
        return request;
    }

    private static ListOffsetsResponseData listOffsetsResponse(String topicName,
                                                               int partition,
                                                               Errors error) {
        var resp = new ListOffsetsResponseData();
        var tr = new ListOffsetsTopicResponse().setName(topicName);
        tr.partitions().add(
                new ListOffsetsPartitionResponse()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        resp.topics().add(tr);
        return resp;
    }

    private static OffsetCommitRequestData offsetCommitRequest(String... topicNames) {
        var request = new OffsetCommitRequestData().setGroupId("test-group");
        for (var name : topicNames) {
            var topic = new OffsetCommitRequestTopic().setName(name);
            topic.partitions().add(new OffsetCommitRequestPartition()
                    .setPartitionIndex(0).setCommittedOffset(0));
            request.topics().add(topic);
        }
        return request;
    }

    private static OffsetCommitResponseData offsetCommitResponse(String topicName,
                                                                 int partition,
                                                                 Errors error) {
        var resp = new OffsetCommitResponseData();
        var tr = new OffsetCommitResponseTopic().setName(topicName);
        tr.partitions().add(
                new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition)
                        .setErrorCode(error.code()));
        resp.topics().add(tr);
        return resp;
    }

    record SentRequest(String route,
                       RequestHeaderData header,
                       ApiMessage body) {}

    /**
     * Routing context that records sent requests and captures
     * the response. Supports per-route backend responses for
     * fan-out testing.
     */
    record SentNodeRequest(VirtualNode virtualNode,
                           RequestHeaderData header,
                           ApiMessage body) {

        int virtualNodeId() {
            return ((TestVirtualNode) virtualNode).encodedId();
        }
    }

    /**
     * Test-specific router response that exposes the body for assertion.
     */
    record TestRouterResponse(
                              @Nullable ApiMessage body,
                              boolean closeConnection)
            implements RouterResponse {}

    /**
     * Test-specific builder implementing the stage interfaces.
     */
    static class TestRouterResponseBuilder implements CloseOrTerminalStage {

        private final @Nullable ApiMessage body;
        private boolean closeConnection;

        TestRouterResponseBuilder(@Nullable ApiMessage body) {
            this.body = body;
        }

        @Override
        public TerminalStage withCloseConnection() {
            closeConnection = true;
            return this;
        }

        @Override
        public RouterResponse build() {
            return new TestRouterResponse(body, closeConnection);
        }

        @Override
        public CompletionStage<RouterResponse> completed() {
            return CompletableFuture.completedStage(build());
        }
    }

    static class CapturingRouterContext implements RouterContext {

        private static final int ANY_NODE_SENTINEL = Integer.MIN_VALUE;

        private final Map<String, ApiMessage> backendResponses;
        private final List<SentRequest> sentRequests = new ArrayList<>();
        private final List<SentNodeRequest> sentNodeRequests = new ArrayList<>();
        private ApiMessage sentResponseBody;
        private Map<VirtualNode, ApiMessage> nodeResponses = Map.of();
        private Subject subject = Subject.anonymous();
        private Optional<VirtualNode> virtualNode = Optional.empty();
        private final Map<String, VirtualNode> routeAnyNodes = new HashMap<>();

        CapturingRouterContext(ApiMessage singleBackendResponse) {
            this.backendResponses = null;
            this.backendResponses_single = singleBackendResponse;
        }

        CapturingRouterContext(Map<String, ? extends ApiMessage> backendResponses) {
            this.backendResponses = new HashMap<>(backendResponses);
            this.backendResponses_single = null;
        }

        private final ApiMessage backendResponses_single;

        CapturingRouterContext withNodeResponses(
                                                 Map<Integer, ? extends ApiMessage> nodeResponses) {
            this.nodeResponses = new HashMap<>();
            nodeResponses.forEach((id, msg) -> this.nodeResponses.put(new TestVirtualNode(id), msg));
            return this;
        }

        CapturingRouterContext withSubject(Subject subject) {
            this.subject = subject;
            return this;
        }

        CapturingRouterContext withVirtualNodeId(int nodeId) {
            this.virtualNode = Optional.of(new TestVirtualNode(nodeId));
            return this;
        }

        List<SentRequest> sentRequests() {
            return sentRequests;
        }

        List<SentNodeRequest> sentNodeRequests() {
            return sentNodeRequests;
        }

        ApiMessage sentResponseBody() {
            return sentResponseBody;
        }

        void captureResult(RouterResponse result) {
            if (result instanceof TestRouterResponse tr && tr.body() != null) {
                sentResponseBody = tr.body();
            }
        }

        @Override
        public CloseOrTerminalStage respondWith(ApiMessage body) {
            return new TestRouterResponseBuilder(body);
        }

        @Override
        public CloseOrTerminalStage respondWith(
                                                ResponseHeaderData header,
                                                ApiMessage body) {
            return new TestRouterResponseBuilder(body);
        }

        @Override
        public CloseOrTerminalStage respondWithError(
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     ApiException exception) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseOrTerminalStage respondWithoutReply() {
            return new TestRouterResponseBuilder(null);
        }

        private MetadataResponseData defaultMetadataResponse = emptyMetadataResponse();
        private Map<String, Integer> findCoordinatorNodeIds = Map.of();

        CapturingRouterContext withDefaultMetadataResponse(MetadataResponseData md) {
            this.defaultMetadataResponse = md;
            return this;
        }

        CapturingRouterContext withFindCoordinatorNodeIds(
                                                          Map<String, Integer> nodeIds) {
            this.findCoordinatorNodeIds = new HashMap<>(nodeIds);
            return this;
        }

        @Override
        public Optional<VirtualNode> virtualNode() {
            return virtualNode;
        }

        @Override
        public VirtualNode anyNode(String route) {
            return routeAnyNodes.computeIfAbsent(route,
                    r -> new TestVirtualNode(ANY_NODE_SENTINEL + routeAnyNodes.size()));
        }

        @Override
        public VirtualNode nodeForId(int virtualNodeId) {
            return new TestVirtualNode(virtualNodeId);
        }

        private String routeForAnyNode(VirtualNode node) {
            return routeAnyNodes.entrySet().stream()
                    .filter(e -> e.getValue().equals(node))
                    .map(Map.Entry::getKey)
                    .findFirst()
                    .orElse(null);
        }

        @Override
        public CompletionStage<ApiMessage> sendRequest(
                                                       VirtualNode node,
                                                       RequestHeaderData header,
                                                       ApiMessage request) {
            String bootstrapRoute = routeForAnyNode(node);
            if (bootstrapRoute != null) {
                sentRequests.add(new SentRequest(bootstrapRoute, header, request));
                if (request instanceof MetadataRequestData) {
                    ApiMessage routeResponse = backendResponses != null
                            ? backendResponses.get(bootstrapRoute)
                            : null;
                    if (routeResponse instanceof MetadataResponseData) {
                        return CompletableFuture.completedFuture(routeResponse);
                    }
                    return CompletableFuture.completedFuture(defaultMetadataResponse);
                }
                if (request instanceof FindCoordinatorRequestData) {
                    ApiMessage routeResponse = backendResponses != null
                            ? backendResponses.get(bootstrapRoute)
                            : null;
                    if (routeResponse instanceof FindCoordinatorResponseData) {
                        return CompletableFuture.completedFuture(routeResponse);
                    }
                    Integer nodeId = findCoordinatorNodeIds.get(bootstrapRoute);
                    var findCoordResp = new FindCoordinatorResponseData()
                            .setErrorCode(Errors.NONE.code())
                            .setNodeId(nodeId != null ? nodeId : 0)
                            .setHost("localhost")
                            .setPort(9092);
                    return CompletableFuture.completedFuture(findCoordResp);
                }
                ApiMessage body;
                if (backendResponses != null) {
                    body = backendResponses.get(bootstrapRoute);
                }
                else {
                    body = backendResponses_single;
                }
                if (body == null) {
                    body = new ProduceResponseData();
                }
                return CompletableFuture.completedFuture(body);
            }
            else {
                sentNodeRequests.add(
                        new SentNodeRequest(node, header, request));
                ApiMessage body = nodeResponses.get(node);
                if (body == null) {
                    body = new ProduceResponseData();
                }
                return CompletableFuture.completedFuture(body);
            }
        }

        @Override
        public String sessionId() {
            return "test-session";
        }

        @Override
        public String topicName(Uuid topicId) {
            return null;
        }

        @Override
        public Subject authenticatedSubject() {
            return subject;
        }
    }

    /**
     * Context for single-route transactional INIT_PRODUCER_ID which goes
     * through three stages: METADATA, FIND_COORDINATOR, INIT_PRODUCER_ID
     * (via sendRequest).
     */
    static class SingleRouteInitCapturingContext implements RouterContext {

        private static final VirtualNode ANY_NODE_SENTINEL_NODE = new TestVirtualNode(Integer.MIN_VALUE);

        private final FindCoordinatorResponseData findCoordResp;
        private final ApiMessage nodeResponse;
        private int sendRequestCount;
        private ApiMessage sentResponseBody;
        private Subject subject = TXN_SUBJECT;

        ApiMessage sentResponseBody() {
            return sentResponseBody;
        }

        void captureResult(RouterResponse result) {
            if (result instanceof TestRouterResponse tr && tr.body() != null) {
                sentResponseBody = tr.body();
            }
        }

        @Override
        public CloseOrTerminalStage respondWith(ApiMessage body) {
            return new TestRouterResponseBuilder(body);
        }

        @Override
        public CloseOrTerminalStage respondWith(
                                                ResponseHeaderData header,
                                                ApiMessage body) {
            return new TestRouterResponseBuilder(body);
        }

        @Override
        public CloseOrTerminalStage respondWithError(
                                                     RequestHeaderData header,
                                                     ApiMessage request,
                                                     ApiException exception) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CloseOrTerminalStage respondWithoutReply() {
            return new TestRouterResponseBuilder(null);
        }

        SingleRouteInitCapturingContext(String route,
                                        int coordinatorNodeId,
                                        FindCoordinatorResponseData findCoordResp,
                                        InitProducerIdResponseData initResp) {
            this.findCoordResp = findCoordResp;
            this.nodeResponse = initResp;
        }

        SingleRouteInitCapturingContext(String route,
                                        ApiMessage nodeResponse,
                                        TopicPartitionRouter router) {
            this.findCoordResp = new FindCoordinatorResponseData().setNodeId(1);
            this.nodeResponse = nodeResponse;
        }

        SingleRouteInitCapturingContext withSubject(Subject subject) {
            this.subject = subject;
            return this;
        }

        @Override
        public Optional<VirtualNode> virtualNode() {
            return Optional.empty();
        }

        @Override
        public VirtualNode anyNode(String route) {
            return ANY_NODE_SENTINEL_NODE;
        }

        @Override
        public VirtualNode nodeForId(int virtualNodeId) {
            return new TestVirtualNode(virtualNodeId);
        }

        @Override
        public CompletionStage<ApiMessage> sendRequest(
                                                       VirtualNode node,
                                                       RequestHeaderData header,
                                                       ApiMessage request) {
            if (node.equals(ANY_NODE_SENTINEL_NODE)) {
                sendRequestCount++;
                ApiMessage body;
                if (request instanceof MetadataRequestData) {
                    body = new MetadataResponseData();
                }
                else if (request instanceof FindCoordinatorRequestData) {
                    body = findCoordResp;
                }
                else {
                    body = new ProduceResponseData();
                }
                return CompletableFuture.completedFuture(body);
            }
            else {
                return CompletableFuture.completedFuture(nodeResponse);
            }
        }

        @Override
        public String sessionId() {
            return "test-session";
        }

        @Override
        public String topicName(Uuid topicId) {
            return null;
        }

        @Override
        public Subject authenticatedSubject() {
            return subject;
        }
    }
}
