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
import freemarker.template.TemplateModelException;

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
                .filter(ms -> MessageSpecType.REQUEST.equals(ms.type()))
                .collect(Collectors.toMap(x -> x.apiKey().orElseThrow(),
                        x -> Set.copyOf(Optional.ofNullable(x.listeners()).orElseThrow())));
    }

    private Set<RequestListenerType> retrieveApiListener(MessageSpecModel messageSpecModel) throws TemplateModelException {
        var spec = (MessageSpec) messageSpecModel.getAdaptedObject(MessageSpec.class);
        var key = spec.apiKey().orElseThrow(() -> new TemplateModelException("Message spec " + spec + " has not API key."));

        return Optional.ofNullable(requestListenerMap.get(key)).orElseThrow(() -> new TemplateModelException("Can's find request spec for API key: " + key));
    }

    @Override
    public Object exec(List arguments) throws TemplateModelException {
        return retrieveApiListener((MessageSpecModel) arguments.get(0));
    }
}
