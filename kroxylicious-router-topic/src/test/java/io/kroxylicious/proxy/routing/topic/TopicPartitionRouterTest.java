/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.DescribeClusterRequestData;
import org.apache.kafka.common.message.DescribeClusterResponseData;
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
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.RoutingContext;

import static org.assertj.core.api.Assertions.assertThat;

class TopicPartitionRouterTest {

    private TopicPartitionRouter router;

    @BeforeEach
    void setUp() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "default-route");
        router = new TopicPartitionRouter(table, "default-route", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");
    }

    // --- close ---

    @Test
    void closeShouldReleaseFetchSessionCacheSlot() {
        var cache = new FetchSessionCache(1000, 10_000, "testVc", "testRouter");
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), "default-route");
        var closeable = new TopicPartitionRouter(table, "default-route",
                new ProducerIdManager(Duration.ofDays(7)), cache, Clock.systemUTC(), "testVc", "testRouter");

        var request = fetchRequest("orders.uk");
        request.setSessionId(0);
        request.setSessionEpoch(0);
        var backendResp = fetchResponse("orders.uk", 0, Errors.NONE);
        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        closeable.onClientRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx);
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
    void shouldCapProduceVersionInApiVersionsResponse() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 13);

        var ctx = new CapturingRoutingContext(responseData);
        router.onClientRequest(
                (short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.PRODUCE))
                .isEqualTo((short) 12);
    }

    @Test
    void shouldCapFetchVersionInApiVersionsResponse() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.FETCH, (short) 16);

        var ctx = new CapturingRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.FETCH))
                .isEqualTo((short) 12);
    }

    @Test
    void shouldNotCapVersionsBelowLimit() {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 10);

        var ctx = new CapturingRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion((ApiVersionsResponseData) ctx.sentResponseBody(), ApiKeys.PRODUCE))
                .isEqualTo((short) 10);
    }

    // --- PRODUCE: single route ---

    @Test
    void shouldRouteProduceToSingleCluster() {
        var request = produceRequest("orders.uk");
        var backendResp = produceResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");
        assertThat(ctx.sentResponseBody()).isInstanceOf(ProduceResponseData.class);
    }

    @Test
    void shouldFanOutProduceAcrossRoutes() {
        var request = produceRequest("orders.uk", "logs.app");
        var respA = produceResponse("orders.uk", 0, Errors.NONE);
        var respB = produceResponse("logs.app", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b");

        var merged = (ProduceResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).extracting("name")
                .containsExactlyInAnyOrder("orders.uk", "logs.app");
    }

    @Test
    void shouldRouteUnmatchedTopicToDefaultRoute() {
        var request = produceRequest("unknown.topic");
        var defaultResp = produceResponse("unknown.topic", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("default-route", defaultResp));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
    }

    @Test
    void shouldSynthesiseErrorForUnroutableTopicsWithNoDefault() {
        var noDefaultTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a"), null);
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");

        var request = produceRequest("orders.uk", "logs.app");
        var respA = produceResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA));
        noDefaultRouter.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", new ProduceResponseData()));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentResponseBody()).isNotNull();
    }

    @Test
    void shouldHandleAcksZeroWithFanOut() {
        var request = produceRequest("orders.uk", "logs.app");
        request.setAcks((short) 0);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", new ProduceResponseData(), "cluster-b", new ProduceResponseData()));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respDefault));
        router.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(
                Map.of("cluster-a", respA, "cluster-b", respFail, "default-route", respDefault));
        router.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx);

        var response = (InitProducerIdResponseData) ctx.sentResponseBody();
        assertThat(response.errorCode()).isEqualTo(Errors.COORDINATOR_NOT_AVAILABLE.code());
    }

    @Test
    void shouldPassThroughInitProducerIdWithSingleRoute() {
        var singleRouteTable = PrefixTopicRoutingTable.create(
                Map.of("orders.", "only-route"), null);
        var singleRouter = new TopicPartitionRouter(singleRouteTable, "only-route", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");

        var request = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000);
        var resp = initProducerIdResponse(42L, (short) 0);

        var ctx = new CapturingRoutingContext(Map.of("only-route", resp));
        singleRouter.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respDefault));
        router.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx);

        var reinitRequest = new InitProducerIdRequestData()
                .setTransactionTimeoutMs(60000)
                .setProducerId(300L)
                .setProducerEpoch((short) 0);
        var reinitRespA = initProducerIdResponse(100L, (short) 1);
        var reinitRespB = initProducerIdResponse(200L, (short) 1);
        var reinitRespDefault = initProducerIdResponse(300L, (short) 1);

        var ctx2 = new CapturingRoutingContext(
                Map.of("cluster-a", reinitRespA, "cluster-b", reinitRespB, "default-route", reinitRespDefault));
        router.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), reinitRequest, ctx2);

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

    // --- idempotent produce ---

    @Test
    void shouldNotRewriteProducerIdForDefaultRoute() {
        setupProducerIdMapping(300L, Map.of(
                "default-route", new ProducerIdManager.ProducerIdEpoch(300L, (short) 0),
                "cluster-a", new ProducerIdManager.ProducerIdEpoch(100L, (short) 0),
                "cluster-b", new ProducerIdManager.ProducerIdEpoch(200L, (short) 0)));

        var request = idempotentProduceRequest(300L, (short) 0, "unknown.topic");
        var resp = produceResponse("unknown.topic", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("default-route", resp));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        var sent = ctx.sentRequests().get(0);
        assertThat(sent.route()).isEqualTo("default-route");
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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", resp));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        var sent = ctx.sentRequests().get(0);
        assertThat(sent.route()).isEqualTo("cluster-a");
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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(2);
        for (var sent : ctx.sentRequests()) {
            var sentReq = (ProduceRequestData) sent.body();
            var records = (MemoryRecords) sentReq.topicData().iterator().next()
                    .partitionData().iterator().next().records();
            var batch = records.batches().iterator().next();
            if (sent.route().equals("cluster-a")) {
                assertThat(batch.producerId()).isEqualTo(100L);
            }
            else if (sent.route().equals("cluster-b")) {
                assertThat(batch.producerId()).isEqualTo(200L);
            }
        }
    }

    @Test
    void shouldReturnUnknownProducerIdWhenMappingMissing() {
        var request = idempotentProduceRequest(999L, (short) 0, "orders.uk", "logs.app");

        var ctx = new CapturingRoutingContext(Map.of());
        router.onClientRequest((short) 12, ApiKeys.PRODUCE, new RequestHeaderData(), request, ctx);

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
        var ctx = new CapturingRoutingContext(responses);
        router.onClientRequest(
                (short) 5, ApiKeys.INIT_PRODUCER_ID, new RequestHeaderData(), request, ctx);
    }

    // --- METADATA ---

    @Test
    void shouldRouteMetadataToSingleCluster() {
        var request = metadataRequest("orders.uk");
        var backendResp = metadataResponse(
                List.of(broker(0, "host-a", 9092)),
                List.of(topicMetadata("orders.uk", 0)));

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        router.onClientRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(
                Map.of("cluster-a", respA, "cluster-b", respB, "default-route", respA));
        router.onClientRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSizeGreaterThanOrEqualTo(2);
        assertThat(ctx.sentResponseBody()).isInstanceOf(MetadataResponseData.class);
    }

    @Test
    void shouldRouteBrokerOnlyMetadataToDefaultRoute() {
        var request = new MetadataRequestData();
        var defaultResp = metadataResponse(
                List.of(broker(0, "host-default", 9092)),
                List.of());
        defaultResp.setClusterId("default-cluster");

        var ctx = new CapturingRoutingContext(Map.of("default-route", defaultResp));
        router.onClientRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 12, ApiKeys.METADATA, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        router.onClientRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");

        var sentReq = (FetchRequestData) ctx.sentRequests().get(0).body();
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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b");

        for (var sent : ctx.sentRequests()) {
            var sentReq = (FetchRequestData) sent.body();
            if (sent.route().equals("cluster-a")) {
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
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");

        var request = fetchRequest("orders.uk", "unknown.topic");
        request.setSessionId(0);
        request.setSessionEpoch(-1);
        var respA = fetchResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA));
        noDefaultRouter.onClientRequest((short) 12, ApiKeys.FETCH, new RequestHeaderData(), request, ctx);

        var merged = (FetchResponseData) ctx.sentResponseBody();
        assertThat(merged.responses()).hasSize(2);
        var unknownResp = merged.responses().stream()
                .filter(t -> t.topic().equals("unknown.topic"))
                .findFirst().orElseThrow();
        assertThat(unknownResp.partitions().get(0).errorCode())
                .isEqualTo(Errors.UNKNOWN_TOPIC_OR_PARTITION.code());
    }

    // --- LIST_OFFSETS ---

    @Test
    void shouldRouteListOffsetsToSingleCluster() {
        var request = listOffsetsRequest("orders.uk");
        var backendResp = listOffsetsResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        router.onClientRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");

        var sentReq = (ListOffsetsRequestData) ctx.sentRequests().get(0).body();
        assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
    }

    @Test
    void shouldFanOutListOffsetsAcrossRoutes() {
        var request = listOffsetsRequest("orders.uk", "logs.app");
        var respA = listOffsetsResponse("orders.uk", 0, Errors.NONE);
        var respB = listOffsetsResponse("logs.app", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b");

        for (var sent : ctx.sentRequests()) {
            var sentReq = (ListOffsetsRequestData) sent.body();
            if (sent.route().equals("cluster-a")) {
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
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");

        var request = listOffsetsRequest("orders.uk", "unknown.topic");
        var respA = listOffsetsResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA));
        noDefaultRouter.onClientRequest((short) 7, ApiKeys.LIST_OFFSETS, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", backendResp));
        router.onClientRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("cluster-a");

        var sentReq = (OffsetCommitRequestData) ctx.sentRequests().get(0).body();
        assertThat(sentReq.groupId()).isEqualTo("test-group");
        assertThat(sentReq.topics()).extracting("name").containsExactly("orders.uk");
    }

    @Test
    void shouldFanOutOffsetCommitAcrossRoutes() {
        var request = offsetCommitRequest("orders.uk", "logs.app");
        var respA = offsetCommitResponse("orders.uk", 0, Errors.NONE);
        var respB = offsetCommitResponse("logs.app", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        router.onClientRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(2);
        assertThat(ctx.sentRequests()).extracting(SentRequest::route)
                .containsExactlyInAnyOrder("cluster-a", "cluster-b");

        for (var sent : ctx.sentRequests()) {
            var sentReq = (OffsetCommitRequestData) sent.body();
            assertThat(sentReq.groupId()).as("groupId preserved on %s", sent.route()).isEqualTo("test-group");
            if (sent.route().equals("cluster-a")) {
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
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"), Clock.systemUTC(), "testVc", "testRouter");

        var request = offsetCommitRequest("orders.uk", "unknown.topic");
        var respA = offsetCommitResponse("orders.uk", 0, Errors.NONE);

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA));
        noDefaultRouter.onClientRequest((short) 9, ApiKeys.OFFSET_COMMIT, new RequestHeaderData(), request, ctx);

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

        var ctx = new CapturingRoutingContext(Map.of("default-route", backendResp));
        router.onClientRequest(
                (short) 3, ApiKeys.FIND_COORDINATOR, new RequestHeaderData(), request, ctx);

        assertThat(ctx.sentRequests()).hasSize(1);
        assertThat(ctx.sentRequests().get(0).route()).isEqualTo("default-route");
        var resp = (FindCoordinatorResponseData) ctx.sentResponseBody();
        assertThat(resp.nodeId()).isEqualTo(1);
    }

    // --- DESCRIBE_CLUSTER ---

    @Test
    void shouldFanOutDescribeClusterAndMergeBrokers() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "cluster-a");
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a",
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter");

        var request = new DescribeClusterRequestData()
                .setIncludeClusterAuthorizedOperations(false);

        var respA = describeClusterResponse("cluster-A", 0, 100);
        respA.brokers().add(describeClusterBroker(0, "hostA-0", 9092));
        respA.brokers().add(describeClusterBroker(1, "hostA-1", 9093));

        var respB = describeClusterResponse("cluster-B", 1, 200);
        respB.brokers().add(describeClusterBroker(2, "hostB-0", 9092));

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        twoRouteRouter.onClientRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx);

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
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a",
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter");

        var request = new DescribeClusterRequestData();

        var respDefault = describeClusterResponse("default-cluster", 42, 100);
        respDefault.setClusterAuthorizedOperations(0xFF);
        respDefault.brokers().add(describeClusterBroker(0, "host0", 9092));

        var respOther = describeClusterResponse("other-cluster", 99, 50);
        respOther.setClusterAuthorizedOperations(0x01);
        respOther.brokers().add(describeClusterBroker(1, "host1", 9092));

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respDefault, "cluster-b", respOther));
        twoRouteRouter.onClientRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx);

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
        var twoRouteRouter = new TopicPartitionRouter(table, "cluster-a",
                new ProducerIdManager(Duration.ofDays(7)),
                new FetchSessionCache(1000, 0, "testVc", "testRouter"),
                Clock.systemUTC(), "testVc", "testRouter");

        var request = new DescribeClusterRequestData();

        var respA = describeClusterResponse("c1", 0, 100);
        respA.brokers().add(describeClusterBroker(0, "h0", 9092));
        var respB = describeClusterResponse("c2", 1, 300);
        respB.brokers().add(describeClusterBroker(1, "h1", 9092));

        var ctx = new CapturingRoutingContext(Map.of("cluster-a", respA, "cluster-b", respB));
        twoRouteRouter.onClientRequest(
                (short) 0, ApiKeys.DESCRIBE_CLUSTER, new RequestHeaderData(), request, ctx);

        var merged = (DescribeClusterResponseData) ctx.sentResponseBody();
        assertThat(merged.throttleTimeMs()).isEqualTo(300);

        twoRouteRouter.close();
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
    static class CapturingRoutingContext implements RoutingContext {

        private final Map<String, ApiMessage> backendResponses;
        private final List<SentRequest> sentRequests = new ArrayList<>();
        private ApiMessage sentResponseBody;

        CapturingRoutingContext(ApiMessage singleBackendResponse) {
            this.backendResponses = null;
            this.backendResponses_single = singleBackendResponse;
        }

        CapturingRoutingContext(Map<String, ? extends ApiMessage> backendResponses) {
            this.backendResponses = new HashMap<>(backendResponses);
            this.backendResponses_single = null;
        }

        private final ApiMessage backendResponses_single;

        List<SentRequest> sentRequests() {
            return sentRequests;
        }

        ApiMessage sentResponseBody() {
            return sentResponseBody;
        }

        @Override
        public CompletionStage<Response> sendRequest(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request) {
            sentRequests.add(new SentRequest(route, header, request));
            ApiMessage body;
            if (backendResponses != null) {
                body = backendResponses.get(route);
            }
            else {
                body = backendResponses_single;
            }
            if (body == null) {
                body = new ProduceResponseData();
            }
            ApiMessage finalBody = body;
            Response response = new SimpleResponse(new ResponseHeaderData(), finalBody);
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public void sendResponse(Response response) {
            sentResponseBody = response.body();
        }

        @Override
        public void disconnect() {
        }

        @Override
        public String sessionId() {
            return "test-session";
        }

        @Override
        public Subject authenticatedSubject() {
            return Subject.anonymous();
        }
    }
}
