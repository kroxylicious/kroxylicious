/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
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
        router = new TopicPartitionRouter(table, "default-route");
    }

    // --- static routes ---

    @Test
    void staticRoutesShouldNotContainDynamicallyRoutedKeys() {
        Map<ApiKeys, String> routes = router.staticRoutes();

        assertThat(routes).doesNotContainKey(ApiKeys.API_VERSIONS);
        assertThat(routes).doesNotContainKey(ApiKeys.PRODUCE);
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
        var noDefaultRouter = new TopicPartitionRouter(noDefaultTable, "cluster-a");

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

    // --- helpers ---

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
