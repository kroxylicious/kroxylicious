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
import org.apache.kafka.common.protocol.Errors;

import com.fasterxml.jackson.databind.JsonNode;

import edu.umd.cs.findbugs.annotations.Nullable;

public record ScenarioDefinition(Metadata metadata, Given given, When when, Then then) {
    public record Metadata(ApiKeys apiKeys, short apiVersion, String scenario) {

    }

    public record MockResponse(ApiKeys expectedRequestKey, short expectedRequestVersion, JsonNode expectedRequestHeader, JsonNode expectedRequest,
                               @Nullable JsonNode upstreamResponseHeader, @Nullable JsonNode upstreamResponse) {

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
     * Expect request future to be completed exceptionally
     * @param withCauseType the type of the expected cause
     * @param withCauseMessage the message of the expected cause
     */
    public record RequestError(String withCauseType, String withCauseMessage) {
        public Class<?> getCauseType() {
            try {
                return this.getClass().getClassLoader().loadClass(withCauseType);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    /**
     * @param expectedResponseHeader
     * @param expectedResponse
     * @param expectedErrorResponse in the case that we expect an error response (the message generation is a framework responsibility)
     * @param hasResponse true if we expect a response (zero-ack produce request is the only case known with no response). default true
     * @param expectRequestDropped true if we expect the request to be dropped. default false
     */
    public record Then(@Nullable JsonNode expectedResponseHeader,
                       @Nullable JsonNode expectedResponse,
                       @Nullable Errors expectedErrorResponse,
                       @Nullable Boolean hasResponse,
                       @Nullable Boolean expectRequestDropped,
                       @Nullable RequestError expectedRequestError) {

        boolean getHasResponse() {
            return hasResponse == null || hasResponse;
        }

        boolean isExpectRequestDropped() {
            return expectRequestDropped != null && expectRequestDropped;
        }
    }
}
