/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.krpccodegen.schema;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a Kafka API: the paired request and response message specifications for a single Kafka RPC.
 *
 * @param name name of the API (request spec name with "Request" suffix removed)
 * @param apiKey the API key enum constant name (e.g. "FETCH", "CREATE_TOPICS")
 * @param listeners the kafka entity(s) that listen for the request (and generate the response)
 * @param request request spec
 * @param response response spec
 */
public record ApiSpec(String name, String apiKey, Set<RequestListenerType> listeners, MessageSpec request, MessageSpec response) implements Named {
    public ApiSpec {
        Objects.requireNonNull(name);
        Objects.requireNonNull(apiKey);
        Objects.requireNonNull(listeners);
        Objects.requireNonNull(request);
        Objects.requireNonNull(response);
    }

    /**
     * Returns true if either the request message spec or the response message spec has at least one field of one of the given entity field types.
     *
     * @param entityTypes entity field types
     * @return true if present, false otherwise
     */
    public boolean hasAtLeastOneEntityField(Set<EntityType> entityTypes) {
        return request().hasAtLeastOneEntityField(entityTypes) || response().hasAtLeastOneEntityField(entityTypes);
    }

    /**
     * Returns true if the request message spec or the response message spec carries a resource list.
     * @return true if present, false otherwise
     */
    public boolean hasResourceList() {
        return request().hasResourceList() || response().hasResourceList();
    }
}
