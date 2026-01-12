/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.krpccodegen.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.krpccodegen.schema.MessageSpec;
import io.kroxylicious.krpccodegen.schema.MessageSpecType;
import io.kroxylicious.krpccodegen.schema.RequestListenerType;

import freemarker.template.TemplateMethodModelEx;

/**
 * A custom FreeMarker function which obtains the listener from a message spec model obejct.
 */
public class RetrieveApiListeners implements TemplateMethodModelEx {

    private final Map<Short, Set<RequestListenerType>> requestListenerMap;

    /**
     * Constructs a RetrieveApiKey.
     */
    public RetrieveApiListeners(Set<MessageSpec> allMessageSpecs) {
        var messageSpecs = Objects.requireNonNull(allMessageSpecs);
        this.requestListenerMap = messageSpecs.stream()
                .filter(ms -> ms.type().equals(MessageSpecType.REQUEST))
                .collect(Collectors.toMap(x -> x.apiKey().orElseThrow(),
                        x -> Set.copyOf(Optional.ofNullable(x.listeners()).orElseThrow())));
    }

    private Set<RequestListenerType> retrieveApiListener(MessageSpecModel messageSpecModel) {
        var key = messageSpecModel.spec.apiKey().orElseThrow();
        return requestListenerMap.get(key);
    }

    @Override
    public Object exec(List arguments) {
        return retrieveApiListener((MessageSpecModel) arguments.get(0));
    }
}
