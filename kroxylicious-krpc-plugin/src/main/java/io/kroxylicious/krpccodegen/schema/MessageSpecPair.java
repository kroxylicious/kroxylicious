/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.Objects;

import org.apache.kafka.common.protocol.ApiKeys;

public record MessageSpecPair(String name, ApiKeys apiKey, java.util.Set<RequestListenerType> listeners, MessageSpec request, MessageSpec response) implements Named {
    public MessageSpecPair {
        Objects.requireNonNull(name);
        Objects.requireNonNull(apiKey);
        Objects.requireNonNull(listeners);
        Objects.requireNonNull(request);
        Objects.requireNonNull(response);
    }
}
