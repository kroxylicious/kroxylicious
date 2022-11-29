/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import java.util.Arrays;

import org.apache.kafka.common.protocol.ApiKeys;

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
    public static DecodePredicate forFilters(KrpcFilter... filters) {
        return new DecodePredicate() {
            @Override
            public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion) {
                for (var filter : filters) {
                    if (filter.shouldDeserializeResponse(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion) {
                for (var filter : filters) {
                    if (filter.shouldDeserializeRequest(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public String toString() {
                return "DecodePredicate$forFilters{" + Arrays.toString(filters) + "}";
            }
        };
    }

    public boolean shouldDecodeRequest(ApiKeys apiKey, short apiVersion);

    public boolean shouldDecodeResponse(ApiKeys apiKey, short apiVersion);

}
