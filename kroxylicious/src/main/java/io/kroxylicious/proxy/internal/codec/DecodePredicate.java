/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.List;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.FilterInvoker;
import io.kroxylicious.proxy.filter.FilterInvokers;
import io.kroxylicious.proxy.filter.KrpcFilter;

/**
 * Encapsulates decisions about whether requests and responses should be
 * fully deserialized into POJOs, or passed through as byte buffers with
 * minimal deserialization.
 *
 * The actual decision can depend on which filters are in use, which can depend on
 * who the authorized user or, or which back-end cluster they're connected to.
 */
public interface DecodePredicate {
    static DecodePredicate forFilters(List<KrpcFilter> filters) {

        List<FilterInvoker> invokers = filters.stream().map(FilterInvokers::from).toList();
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
                return "DecodePredicate$forFilters{" + filters + "}";
            }
        };
    }

    boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion);

    boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion);

}
