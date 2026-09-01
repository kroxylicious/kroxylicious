/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.integration.codec;

import java.util.concurrent.CompletableFuture;

import io.kroxylicious.kafka.common.protocol.ApiKeys;

import io.kroxylicious.testing.integration.client.SequencedResponse;

/**
 * A frame in the Kafka protocol carrying a request.
 */
public interface RequestFrame extends Frame {

    /**
     * The future that will be completed with the response to this request.
     * @return the response future
     */
    CompletableFuture<SequencedResponse> getResponseFuture();

    /**
     * Whether the Kafka Client expects a response to this request
     * @return Whether the Kafka Client expects a response to this request
     */
    default boolean hasResponse() {
        return true;
    }

    /**
     * Get apiKey of body
     * @return apiKey
     */
    ApiKeys apiKey();

    /**
     * Get apiVersion of frame
     * @return apiVersion
     */
    short apiVersion();

    /**
     * The API version with which the response to this request should be decoded
     * @return the response API version, defaults to {@link #apiVersion()}
     */
    default short responseApiVersion() {
        return apiVersion();
    }

}
