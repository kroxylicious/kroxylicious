/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.Arrays;

import org.apache.kafka.common.protocol.ApiKeys;

import io.kroxylicious.proxy.filter.KrpcFilter;
import io.kroxylicious.proxy.invoker.FilterInvoker;
import io.kroxylicious.proxy.invoker.FilterInvokers;

/**
 * Encapsulates decisions about whether requests and responses should be
 * fully deserialized into POJOs, or passed through as byte buffers with
 * minimal deserialization.
 *
 * The actual decision can depend on which filters are in use, which can depend on
 * who the authorized user or, or which back-end cluster they're connected to.
 */
public interface DecodePredicate {

    static DecodePredicate forFilters(KrpcFilter... filters) {
        return forInvokers(FilterInvokers.invokersFor(filters));
    }

    static DecodePredicate forInvokers(FilterInvoker... invokers) {
        return new DecodePredicate() {
            @Override
            public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
                for (var invoker : invokers) {
                    if (invoker.shouldDeserializeResponse(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
                for (var invoker : invokers) {
                    if (invoker.shouldDeserializeRequest(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                return "DecodePredicate$forInvokers{" + Arrays.toString(invokers) + "}";
            }
        };
    }

    boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion);

    boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion);

}
