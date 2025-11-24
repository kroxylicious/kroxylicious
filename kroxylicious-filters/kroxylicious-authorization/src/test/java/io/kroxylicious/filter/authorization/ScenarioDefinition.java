/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.authorization;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.ApiKeys;

import com.fasterxml.jackson.databind.JsonNode;

import edu.umd.cs.findbugs.annotations.Nullable;

public record ScenarioDefinition(Metadata metadata, Given given, When when, Then then) {
    public record Metadata(ApiKeys apiKeys, short apiVersion, String scenario) {

    }

    public record MockResponse(ApiKeys expectedRequestKey, short expectedRequestVersion, JsonNode expectedRequestHeader, JsonNode expectedRequest,
                               JsonNode upstreamResponseHeader, JsonNode upstreamResponse) {

    }

    /**
     * @param mockedUpstreamResponses response for mock upstream to respond with, empty if expect no requests to be sent upstream
     * @param authorizerRules rules
     * @param topicNames topic id to topic name map
     */
    public record Given(List<MockResponse> mockedUpstreamResponses, SimpleAuthorizer.Config authorizerRules,
                        @Nullable Map<Uuid, String> topicNames) {}

    /**
     * @param subject subject sending the request
     * @param requestHeader header to send
     * @param request request to send
     */
    public record When(String subject, JsonNode requestHeader, JsonNode request) {}

    /**
     * @param expectedResponseHeader
     * @param expectedResponse
     */
    public record Then(@Nullable JsonNode expectedResponseHeader, @Nullable JsonNode expectedResponse) {}
}
