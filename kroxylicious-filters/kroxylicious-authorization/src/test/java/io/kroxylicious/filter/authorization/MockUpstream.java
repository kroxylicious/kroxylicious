/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.apache.kafka.common.message.RequestHeaderData;
import org.apache.kafka.common.message.RequestHeaderDataJsonConverter;
import org.apache.kafka.common.message.ResponseHeaderData;
import org.apache.kafka.common.message.ResponseHeaderDataJsonConverter;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.ApiMessage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import io.kroxylicious.test.requestresponsetestdef.KafkaApiMessageConverter;

import static org.assertj.core.api.Assertions.assertThat;

public class MockUpstream {
    private static final ObjectMapper MAPPER = new ObjectMapper(new YAMLFactory());
    private final List<ScenarioDefinition.MockResponse> mockResponses;

    public MockUpstream(List<ScenarioDefinition.MockResponse> mockResponses) {
        this.mockResponses = mockResponses;
    }

    public <M extends ApiMessage> CompletionStage<M> sendRequest(RequestHeaderData header, ApiMessage request) {
        if (mockResponses.isEmpty()) {
            throw new IllegalStateException("No mock responses remaining!");
        }
        try {
            Response response = respond(header, request);
            return CompletableFuture.completedFuture((M) response.message());
        }
        catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private ScenarioDefinition.MockResponse pop() {
        return this.mockResponses.remove(0);
    }

    private static String toYaml(JsonNode actualBody) {
        try {
            return MAPPER.writer().writeValueAsString(actualBody);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isFinished() {
        return mockResponses.isEmpty();
    }

    public Response respond(RequestHeaderData requestHeader, ApiMessage request) {
        ScenarioDefinition.MockResponse mockDefinition = pop();
        ApiKeys key = mockDefinition.expectedRequestKey();
        assertThat(key).isEqualTo(ApiKeys.forId(requestHeader.requestApiKey()));
        short version = mockDefinition.expectedRequestVersion();
        assertThat(requestHeader.requestApiVersion()).isEqualTo(version);
        JsonNode actualHeader = RequestHeaderDataJsonConverter.write(requestHeader, key.requestHeaderVersion(version));
        assertThat(toYaml(actualHeader)).isEqualTo(toYaml(mockDefinition.expectedRequestHeader()));
        JsonNode actualBody = KafkaApiMessageConverter.requestConverterFor(key.messageType).writer().apply(request, version);
        assertThat(toYaml(actualBody)).isEqualTo(toYaml(mockDefinition.expectedRequest()));
        JsonNode jsonNode = mockDefinition.upstreamResponse();
        ResponseHeaderData header = ResponseHeaderDataJsonConverter.read(mockDefinition.upstreamResponseHeader(), key.requestHeaderVersion(version));
        ApiMessage responseMessage = KafkaApiMessageConverter.responseConverterFor(key.messageType).reader().apply(jsonNode, version);
        return new Response(header, responseMessage);
    }

    public record Response(ResponseHeaderData header, ApiMessage message) {}
}
