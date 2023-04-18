/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.test.ApiMessageSampleGenerator;
import io.kroxylicious.test.ApiMessageSampleGenerator.ApiAndVersion;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.server.MockServer;

import static io.kroxylicious.proxy.Utils.startProxy;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class ProxyRpcTest {

    public static final int PROXY_PORT = 9192;
    public static final String PROXY_ADDRESS = "localhost:" + PROXY_PORT;
    private static MockServer mockServer;
    private static KafkaClient kafkaClient;
    private static KafkaProxy kafkaProxy;

    public record Scenario(String name, Response givenMockResponse, Request whenSendRequest, Request thenMockReceivesRequest, Response thenResponseReceived) {
    }

    /**
     * API_VERSIONS is not proxied, kroxy can respond to this itself
     */
    private static final Set<ApiKeys> NOT_PROXIED = Set.of(API_VERSIONS);

    @BeforeAll
    public static void beforeAll() throws InterruptedException {
        mockServer = MockServer.startOnRandomPort();
        kafkaClient = new KafkaClient();
        String config = configure(mockServer);
        kafkaProxy = startProxy(config);
    }

    @MethodSource("scenarios")
    @ParameterizedTest
    public void testKroxyliciousCanDecodeManipulateAndProxyRPC(Scenario scenario) throws Exception {
        mockServer.clear();
        mockServer.setResponse(scenario.givenMockResponse());
        CompletableFuture<Response> future = kafkaClient.get("localhost", PROXY_PORT, scenario.whenSendRequest());
        Response response = future.get(10, TimeUnit.SECONDS);
        assertEquals(scenario.thenMockReceivesRequest(), onlyRequest(mockServer), "unexpected request received at mock for scenario: " + scenario.name());
        assertEquals(scenario.thenResponseReceived(), response, "unexpected response received from kroxylicious for scenario: " + scenario.name());
    }

    @AfterAll
    public static void afterAll() throws Exception {
        kafkaProxy.close();
        mockServer.close();
        kafkaClient.close();
    }

    @NotNull
    private static Stream<Scenario> scenarios() {
        Map<ApiAndVersion, ApiMessage> requestSamples = ApiMessageSampleGenerator.createRequestSamples();
        Map<ApiAndVersion, ApiMessage> responseSamples = ApiMessageSampleGenerator.createResponseSamples();
        return Arrays.stream(ApiKeys.values()).filter(apiKeys -> !NOT_PROXIED.contains(apiKeys)).flatMap(apiKeys -> toScenario(requestSamples, responseSamples, apiKeys));
    }

    private static final ApiAndVersion v0HeaderVersion = new ApiAndVersion(CONTROLLED_SHUTDOWN, (short) 0);

    private static @NotNull Stream<Scenario> toScenario(Map<ApiAndVersion, ApiMessage> requestSamples, Map<ApiAndVersion, ApiMessage> responseSample, ApiKeys apiKeys) {
        ApiMessageType messageType = apiKeys.messageType;
        IntStream supported = IntStream.range(messageType.lowestSupportedVersion(), apiKeys.messageType.highestSupportedVersion() + 1);
        return supported.mapToObj(version -> new ApiAndVersion(apiKeys, (short) version)).map(apiAndVersion -> {
            ApiMessage request = requestSamples.get(apiAndVersion);
            ApiMessage response = responseSample.get(apiAndVersion);
            Request clientRequest = createRequestDefinition(apiAndVersion, "mockClientId", request);
            String expected;
            if (v0HeaderVersion.equals(apiAndVersion)) {
                // controlled shutdown is the only usage of a version 0 header schema which doesn't have clientId
                expected = "";
            }
            else {
                expected = "fixed";
            }
            Request expectedAtMock = createRequestDefinition(apiAndVersion, expected, request);
            Response responseJson = createResponseDefinition(apiAndVersion, response);
            return new Scenario(apiKeys.name, responseJson, clientRequest, expectedAtMock, responseJson);
        });
    }

    @NotNull
    private static Response createResponseDefinition(ApiAndVersion apiAndVersion, ApiMessage message) {
        return new Response(apiAndVersion.keys(), apiAndVersion.apiVersion(), message);
    }

    @NotNull
    private static Request createRequestDefinition(ApiAndVersion apiKeys, String clientId, ApiMessage requestBody) {
        return new Request(apiKeys.keys(), apiKeys.apiVersion(), clientId, requestBody);
    }

    private static String configure(MockServer mockServer) {
        return KroxyConfig.builder()
                .addToVirtualClusters("demo", new VirtualClusterBuilder()
                        .withNewTargetCluster()
                        .withBootstrapServers("localhost:" + mockServer.port())
                        .endTargetCluster()
                        .withNewClusterEndpointConfigProvider()
                        .withType("StaticCluster")
                        .withConfig(Map.of("bootstrapAddress", ProxyRpcTest.PROXY_ADDRESS))
                        .endClusterEndpointConfigProvider()
                        .build())
                .addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("FixedClientId").withConfig(Map.of("clientId", "fixed")).endFilter()
                .build().toYaml();
    }

    private static Request onlyRequest(MockServer mockServer) {
        List<Request> requests = mockServer.getReceivedRequests();
        if (requests.size() != 1) {
            fail("mock server does not have exactly one request recorded");
        }
        return requests.get(0);
    }

}
