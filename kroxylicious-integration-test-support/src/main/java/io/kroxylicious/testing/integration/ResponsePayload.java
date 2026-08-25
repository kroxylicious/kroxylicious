/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration;

import io.kroxylicious.kafka.common.protocol.ApiKeys;
import io.kroxylicious.kafka.common.protocol.ApiMessage;

/**
 * The payload of a Kafka response.
 *
 * @param apiKeys the api key of the response
 * @param apiVersion the api version of the request the response answers
 * @param message the response body
 * @param responseApiVersion the api version with which the response is encoded
 */
public record ResponsePayload(ApiKeys apiKeys,
                              short apiVersion,
                              ApiMessage message,
                              short responseApiVersion) {

    /**
     * Creates a ResponsePayload encoded with the same api version as the request.
     *
     * @param apiKeys the api key of the response
     * @param apiVersion the api version of the request the response answers
     * @param message the response body
     */
    public ResponsePayload(ApiKeys apiKeys, short apiVersion, ApiMessage message) {
        this(apiKeys, apiVersion, message, apiVersion);
    }

}
