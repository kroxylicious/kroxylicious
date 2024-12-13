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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import io.github.nettyplus.leakdetector.junit.NettyLeakDetectorExtension;

import io.kroxylicious.proxy.config.FilterDefinitionBuilder;
import io.kroxylicious.proxy.filter.FixedClientIdFilterFactory;
import io.kroxylicious.test.ApiMessageSampleGenerator;
import io.kroxylicious.test.ApiMessageSampleGenerator.ApiAndVersion;
import io.kroxylicious.test.Request;
import io.kroxylicious.test.Response;
import io.kroxylicious.test.ResponsePayload;
import io.kroxylicious.test.client.KafkaClient;
import io.kroxylicious.test.tester.MockServerKroxyliciousTester;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.test.tester.KroxyliciousConfigUtils.proxy;
import static io.kroxylicious.test.tester.KroxyliciousTesters.mockKafkaKroxyliciousTester;
import static org.apache.kafka.common.protocol.ApiKeys.API_VERSIONS;
import static org.apache.kafka.common.protocol.ApiKeys.CONTROLLED_SHUTDOWN;
import static org.apache.kafka.common.protocol.ApiKeys.DESCRIBE_CLUSTER;
import static org.apache.kafka.common.protocol.ApiKeys.FIND_COORDINATOR;
import static org.apache.kafka.common.protocol.ApiKeys.METADATA;
import static org.apache.kafka.common.protocol.ApiKeys.SHARE_ACKNOWLEDGE;
import static org.apache.kafka.common.protocol.ApiKeys.SHARE_FETCH;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(NettyLeakDetectorExtension.class)
public class ProxyRpcTest {
    private static MockServerKroxyliciousTester mockTester;

    public record Scenario(String name, ResponsePayload givenMockResponse, Request whenSendRequest, Request thenMockReceivesRequest,
                           ResponsePayload thenResponseReceived) {}

    /**
     * API_VERSIONS is not proxied, kroxylicious can respond to this itself
     * FIND_COORDINATOR, METADATA, DESCRIBE_CLUSTER, kroxylicious takes charge of rewriting these responses itself.
     */
    private static final Set<ApiKeys> SKIPPED_API_KEYS = Set.of(API_VERSIONS, FIND_COORDINATOR, METADATA, DESCRIBE_CLUSTER, SHARE_ACKNOWLEDGE, SHARE_FETCH);

    @BeforeAll
    public static void beforeAll() {
        mockTester = mockKafkaKroxyliciousTester(mockBootstrap -> proxy(mockBootstrap)
                .addToFilters(new FilterDefinitionBuilder(FixedClientIdFilterFactory.class.getName()).withConfig("clientId", "fixed").build()));
    }

    @BeforeEach
    void setup() {
        mockTester.clearMock();
    }

    @AfterAll
    public static void afterAll() {
        if (mockTester != null) {
            mockTester.close();
        }
    }

    @MethodSource("scenarios")
    @ParameterizedTest
    void testKroxyliciousCanDecodeManipulateAndProxyRPC(Scenario scenario) {
        mockTester.addMockResponseForApiKey(scenario.givenMockResponse());
        try (KafkaClient kafkaClient = mockTester.simpleTestClient()) {
            Response response = kafkaClient.getSync(scenario.whenSendRequest());
            assertEquals(scenario.thenMockReceivesRequest(), mockTester.getOnlyRequest(), "unexpected request received at mock for scenario: " + scenario.name());
            assertEquals(scenario.thenResponseReceived(), response.payload(), "unexpected response received from kroxylicious for scenario: " + scenario.name());
        }
    }

    @NonNull
    private static Stream<Scenario> scenarios() {
        Map<ApiAndVersion, ApiMessage> requestSamples = ApiMessageSampleGenerator.createRequestSamples();
        Map<ApiAndVersion, ApiMessage> responseSamples = ApiMessageSampleGenerator.createResponseSamples();
        return Arrays.stream(ApiKeys.values()).filter(apiKeys -> !SKIPPED_API_KEYS.contains(apiKeys))
                .flatMap(apiKeys -> toScenario(requestSamples, responseSamples, apiKeys));
    }

    private static final ApiAndVersion v0HeaderVersion = new ApiAndVersion(CONTROLLED_SHUTDOWN, (short) 0);

    private static @NonNull Stream<Scenario> toScenario(Map<ApiAndVersion, ApiMessage> requestSamples, Map<ApiAndVersion, ApiMessage> responseSample, ApiKeys apiKeys) {
        ApiMessageType messageType = apiKeys.messageType;
        IntStream supported = IntStream.range(messageType.lowestSupportedVersion(), apiKeys.messageType.highestSupportedVersion(true) + 1);
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
            ResponsePayload responseJson = createResponseDefinition(apiAndVersion, response);
            return new Scenario(apiKeys.name, responseJson, clientRequest, expectedAtMock, responseJson);
        });
    }

    @NonNull
    private static ResponsePayload createResponseDefinition(ApiAndVersion apiAndVersion, ApiMessage message) {
        return new ResponsePayload(apiAndVersion.keys(), apiAndVersion.apiVersion(), message);
    }

    @NonNull
    private static Request createRequestDefinition(ApiAndVersion apiKeys, String clientId, ApiMessage requestBody) {
        return new Request(apiKeys.keys(), apiKeys.apiVersion(), clientId, requestBody);
    }

}
