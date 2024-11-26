/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.frame;

import java.util.HashMap;
import java.util.Map;

public class LazyRequestResponseState implements RequestResponseState {

    Map<Object, Object> state = null;

    private LazyRequestResponseState() {
    }

    @Override
    public <K, V> void putState(K key, V value) {
        if (state == null) {
            state = new HashMap<>();
        }
        state.put(key, value);
    }

    @Override
    public <K, V> V getStateOrDefault(K key, V defaultValue) {
        if (state == null) {
            return defaultValue;
        }
        else {
            Object o = state.get(key);
            if (o == null) {
                return defaultValue;
            }
            if (!defaultValue.getClass().isAssignableFrom(o.getClass())) {
                throw new IllegalStateException(
                        "value class " + defaultValue.getClass() + " is not assignable from " + o.getClass() + " unexpected type in the state map");
            }
            // noinspection unchecked
            return (V) defaultValue.getClass().cast(o);
        }
    }

    public static RequestResponseState lazyRequestResponseState() {
        return new LazyRequestResponseState();
    }
}
