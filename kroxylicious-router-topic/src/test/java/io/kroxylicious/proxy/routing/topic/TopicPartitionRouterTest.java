/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.InitProducerIdRequestData;
import org.apache.kafka.common.message.InitProducerIdResponseData;
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
        router = new TopicPartitionRouter(table, "default-route", new ProducerIdManager(Duration.ofDays(7)));
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

        assertThat(routes).containsEntry(ApiKeys.FETCH, "default-route");
        assertThat(routes).containsEntry(ApiKeys.METADATA, "default-route");
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
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a", new ProducerIdManager(Duration.ofDays(7)));

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
        var singleRouter = new TopicPartitionRouter(singleRouteTable, "only-route", new ProducerIdManager(Duration.ofDays(7)));

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

    // --- helpers ---

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
