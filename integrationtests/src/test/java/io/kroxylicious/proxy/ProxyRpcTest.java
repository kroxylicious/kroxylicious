/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.kafka.common.message.ApiMessageType;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.test.ApiMessageSampleGenerator;
import io.kroxylicious.test.ApiMessageSampleGenerator.ApiAndVersion;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProxyRpcTest {
    private static MockServerKroxyliciousTester mockTester;

    public record Scenario(String name, Response givenMockResponse, Request whenSendRequest, Request thenMockReceivesRequest, Response thenResponseReceived) {
    }

    /**
     * API_VERSIONS is not proxied, kroxy can respond to this itself
     */
    private static final Set<ApiKeys> NOT_PROXIED = Set.of(API_VERSIONS);

    @BeforeAll
    public static void beforeAll() {
        mockTester = mockKafkaKroxyliciousTester((mockBootstrap) -> proxy(mockBootstrap).addNewFilter().withType("ApiVersions").endFilter()
                .addNewFilter().withType("FixedClientId").withConfig(Map.of("clientId", "fixed")).endFilter());
    }

    @BeforeEach
    public void setup() {
        mockTester.clearMock();
    }

    @AfterAll
    public static void afterAll() {
        mockTester.close();
    }

    @MethodSource("scenarios")
    @ParameterizedTest
    public void testKroxyliciousCanDecodeManipulateAndProxyRPC(Scenario scenario) {
        mockTester.setMockResponse(scenario.givenMockResponse());
        try (KafkaClient kafkaClient = mockTester.singleRequestClient()) {
            Response response = kafkaClient.getSync(scenario.whenSendRequest());
            assertEquals(scenario.thenMockReceivesRequest(), mockTester.onlyRequest(), "unexpected request received at mock for scenario: " + scenario.name());
            assertEquals(scenario.thenResponseReceived(), response, "unexpected response received from kroxylicious for scenario: " + scenario.name());
        }
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

}
