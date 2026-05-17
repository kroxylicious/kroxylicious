/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.routing.topic;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.ApiVersionsResponseData;
import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.routing.Response;
import io.kroxylicious.proxy.routing.RoutingContext;
import io.kroxylicious.proxy.routing.RoutingResult;

import static org.assertj.core.api.Assertions.assertThat;

class TopicPartitionRouterTest {

    private TopicPartitionRouter router;

    @BeforeEach
    void setUp() {
        var table = PrefixTopicRoutingTable.create(
                Map.of("orders.", "cluster-a", "logs.", "cluster-b"), "default-route");
        router = new TopicPartitionRouter(table, "default-route");
    }

    @Test
    void staticRoutesShouldContainAllNonDynamicApiKeys() {
        Map<ApiKeys, String> routes = router.staticRoutes();

        assertThat(routes).doesNotContainKey(ApiKeys.API_VERSIONS);

        for (ApiKeys key : ApiKeys.values()) {
            if (key != ApiKeys.API_VERSIONS) {
                assertThat(routes).containsEntry(key, "default-route");
            }
        }
    }

    @Test
    void shouldCapProduceVersionInApiVersionsResponse() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 13);

        var ctx = new StubRoutingContext(responseData);
        CompletionStage<RoutingResult> result = router.onClientRequest(
                (short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(result.toCompletableFuture()).isCompleted();
        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.PRODUCE)).isEqualTo((short) 12);
    }

    @Test
    void shouldCapFetchVersionInApiVersionsResponse() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.FETCH, (short) 16);

        var ctx = new StubRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.FETCH)).isEqualTo((short) 12);
    }

    @Test
    void shouldCapOffsetCommitVersion() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.OFFSET_COMMIT, (short) 10);

        var ctx = new StubRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.OFFSET_COMMIT)).isEqualTo((short) 9);
    }

    @Test
    void shouldCapOffsetFetchVersion() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.OFFSET_FETCH, (short) 10);

        var ctx = new StubRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.OFFSET_FETCH)).isEqualTo((short) 9);
    }

    @Test
    void shouldCapDeleteTopicsVersion() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.DELETE_TOPICS, (short) 6);

        var ctx = new StubRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.DELETE_TOPICS)).isEqualTo((short) 5);
    }

    @Test
    void shouldNotCapVersionsBelowLimit() throws Exception {
        var responseData = apiVersionsResponse();
        setMaxVersion(responseData, ApiKeys.PRODUCE, (short) 10);

        var ctx = new StubRoutingContext(responseData);
        router.onClientRequest((short) 3, ApiKeys.API_VERSIONS, new RequestHeaderData(), responseData, ctx);

        assertThat(findMaxVersion(ctx.sentResponse(), ApiKeys.PRODUCE)).isEqualTo((short) 10);
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

    /**
     * Minimal stub that captures the response sent by the router.
     */
    static class StubRoutingContext implements RoutingContext {

        private final ApiVersionsResponseData backendResponse;
        private ApiVersionsResponseData sentResponse;

        StubRoutingContext(ApiVersionsResponseData backendResponse) {
            this.backendResponse = backendResponse;
        }

        ApiVersionsResponseData sentResponse() {
            return sentResponse;
        }

        @Override
        public CompletionStage<Response> sendRequest(String route,
                                                     RequestHeaderData header,
                                                     ApiMessage request) {
            Response response = new Response() {
                @Override
                public org.apache.kafka.common.message.ResponseHeaderData header() {
                    return new org.apache.kafka.common.message.ResponseHeaderData();
                }

                @Override
                public ApiMessage body() {
                    return backendResponse;
                }
            };
            return CompletableFuture.completedFuture(response);
        }

        @Override
        public void sendResponse(Response response) {
            sentResponse = (ApiVersionsResponseData) response.body();
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
