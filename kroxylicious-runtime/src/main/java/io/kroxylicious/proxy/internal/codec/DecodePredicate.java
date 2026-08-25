/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.List;

import io.kroxylicious.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.internal.filter.FilterAndInvoker;
import io.kroxylicious.proxy.internal.filter.FilterInvoker;

/**
 * Encapsulates decisions about whether requests and responses should be
 * fully deserialized into POJOs, or passed through as byte buffers with
 * minimal deserialization.
 *
 * The actual decision can depend on which filters are in use, which can depend on
 * who the authorized user or, or which back-end cluster they're connected to.
 */
public interface DecodePredicate {
    /**
     * Creates a predicate that decodes a request or response if any of the given filters would handle it.
     * @param filterAndInvokers The filters in use.
     * @return The predicate.
     */
    static DecodePredicate forFilters(List<FilterAndInvoker> filterAndInvokers) {

        List<FilterInvoker> invokers = filterAndInvokers.stream().map(FilterAndInvoker::invoker).toList();
        return new DecodePredicate() {
            @Override
            public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
                for (var invoker : invokers) {
                    if (invoker.shouldHandleResponse(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
                for (var invoker : invokers) {
                    if (invoker.shouldHandleRequest(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                return "DecodePredicate$forFilters{" + filterAndInvokers + "}";
            }
        };
    }

    /**
     * Whether a request with the given api key and version should be fully decoded.
     * @param apiKey The api key of the request.
     * @param apiVersion The api version of the request.
     * @return true if the request should be decoded, false otherwise.
     */
    boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion);

    /**
     * Whether a response with the given api key and version should be fully decoded.
     * @param apiKey The api key of the response.
     * @param apiVersion The api version of the response.
     * @return true if the response should be decoded, false otherwise.
     */
    boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion);

}
