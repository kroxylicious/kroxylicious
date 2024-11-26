/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

/**
 * Represents additional state associated with a single request-response pair.
 * This is to enable the Runtime, and perhaps later Filters, to attach state to
 * a Request-Response pair.
 */
public interface RequestResponseState {

    <K, V> void putState(K key, V value);

    <K, V> V getStateOrDefault(K key, V defaultValue);

    static RequestResponseState empty() {
        return new RequestResponseState() {
            @Override
            public <K, V> void putState(K key, V value) {
                throw new UnsupportedOperationException("empty request response state is immutable");
            }

            @Override
            public <K, V> V getStateOrDefault(K key, V defaultValue) {
                return defaultValue;
            }
        };
    }
}
