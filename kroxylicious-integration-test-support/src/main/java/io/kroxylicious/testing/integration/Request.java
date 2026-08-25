/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * A Kafka request to be sent by the test client.
 *
 * @param apiKeys the api key of the request
 * @param apiVersion the api version with which to encode the request
 * @param clientIdHeader the client id to set in the request header
 * @param message the request body
 * @param responseApiVersion the api version with which the response should be decoded
 */
public record Request(ApiKeys apiKeys,
                      short apiVersion,
                      String clientIdHeader,
                      ApiMessage message,
                      short responseApiVersion) {
    /**
     * Creates a Request whose response is expected with the same api version as the request.
     *
     * @param apiKeys the api key of the request
     * @param apiVersion the api version with which to encode the request
     * @param clientIdHeader the client id to set in the request header
     * @param message the request body
     */
    public Request(ApiKeys apiKeys,
                   short apiVersion,
                   String clientIdHeader,
                   ApiMessage message) {
        this(apiKeys, apiVersion, clientIdHeader, message, apiVersion);
    }
}
