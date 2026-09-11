/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a Kafka API: the paired request and response message specifications for a single Kafka RPC.
 * All derived fields (name, apiKey, kafkaApiKeyEnumName, listeners) are extracted from the request
 * and frozen at construction time. The constructor validates that the request and response pair correctly.
 */
public final class ApiSpec implements Named {

    private final String name;
    private final short apiKey;
    private final String kafkaApiKeyEnumName;
    private final Set<RequestListenerType> listeners;
    private final MessageSpec request;
    private final MessageSpec response;

    public ApiSpec(MessageSpec request, MessageSpec response) {
        Objects.requireNonNull(request);
        Objects.requireNonNull(response);
        if (!request.name().endsWith("Request")) {
            throw new IllegalArgumentException(
                    "Request name " + request.name() + " does not end with 'Request'");
        }
        if (!response.name().endsWith("Response")) {
            throw new IllegalArgumentException(
                    "Response name " + response.name() + " does not end with 'Response'");
        }
        if (!request.apiKey().equals(response.apiKey())) {
            throw new IllegalArgumentException(
                    "Request apiKey " + request.apiKey() + " does not match response apiKey " + response.apiKey());
        }
        this.request = request;
        this.response = response;
        this.name = request.name().replaceFirst("Request$", "");
        this.apiKey = request.apiKey().orElseThrow(() -> new IllegalStateException("Request " + request.name() + " has no apiKey"));
        this.kafkaApiKeyEnumName = request.kafkaApiKeyEnumName();
        var l = request.listeners();
        this.listeners = l == null ? Set.of() : Set.copyOf(new HashSet<>(l));
    }

    @Override
    public String name() {
        return name;
    }

    public short apiKey() {
        return apiKey;
    }

    public String kafkaApiKeyEnumName() {
        return kafkaApiKeyEnumName;
    }

    public Set<RequestListenerType> listeners() {
        return listeners;
    }

    public MessageSpec request() {
        return request;
    }

    public MessageSpec response() {
        return response;
    }

    /**
     * Returns true if either the request message spec or the response message spec has at least one field of one of the given entity field types.
     *
     * @param entityTypes entity field types
     * @return true if present, false otherwise
     */
    public boolean hasAtLeastOneEntityField(Set<EntityType> entityTypes) {
        return request.hasAtLeastOneEntityField(entityTypes) || response.hasAtLeastOneEntityField(entityTypes);
    }

    /**
     * Returns true if the request message spec or the response message spec carries a resource list.
     * @return true if present, false otherwise
     */
    public boolean hasResourceList() {
        return request.hasResourceList() || response.hasResourceList();
    }
}
