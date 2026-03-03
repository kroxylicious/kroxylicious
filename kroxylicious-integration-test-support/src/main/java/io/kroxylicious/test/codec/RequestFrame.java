/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.codec;

import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.test.client.SequencedResponse;

public interface RequestFrame extends Frame {

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
     * @return apiKey
     */
    short apiVersion();

    default short responseApiVersion() {
        return apiVersion();
    }

}
