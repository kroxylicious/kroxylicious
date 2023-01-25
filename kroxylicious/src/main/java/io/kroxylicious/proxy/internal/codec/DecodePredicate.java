/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.codec;

import org.apache.kafka.common.protocol.ApiKeys;

/**
 * Encapsulates decisions about whether requests and responses should be
 * fully deserialized into POJOs, or passed through as byte buffers with
 * minimal deserialization.
 *
 * The actual decision can depend on which filters are in use, which can depend on
 * who the authorized user or, or which back-end cluster they're connected to.
 */
public interface DecodePredicate {

    static DecodePredicate anyOf(DecodePredicate[] predicates) {
        return new DecodePredicate() {
            @Override
            public boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion) {
                for (var invoker : predicates) {
                    if (invoker.shouldDeserializeResponse(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }

            @Override
            public boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion) {
                for (var invoker : predicates) {
                    if (invoker.shouldDeserializeRequest(apiKey, apiVersion)) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    boolean shouldDeserializeRequest(ApiKeys apiKey, short apiVersion);

    boolean shouldDeserializeResponse(ApiKeys apiKey, short apiVersion);

}
