/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.MessageSpecType;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;

public final class MessageSpecFilters {

    private MessageSpecFilters() {
        // static utility class
    }

    public static Stream<MessageSpec> filterByRequestListener(Set<MessageSpec> allMessageSpecs, RequestListenerType requestListenerType) {
        Objects.requireNonNull(allMessageSpecs);
        Objects.requireNonNull(requestListenerType);
        var requestListenerMap = allMessageSpecs.stream()
                .filter(ms -> MessageSpecType.REQUEST.equals(ms.type()))
                .collect(Collectors.toMap(x -> x.apiKey().orElseThrow(),
                        x -> Set.copyOf(Optional.ofNullable(x.listeners()).orElseThrow())));
        return allMessageSpecs.stream()
                .filter(ms -> requestListenerMap.get(ms.apiKey().orElseThrow()).contains(requestListenerType));
    }
}
