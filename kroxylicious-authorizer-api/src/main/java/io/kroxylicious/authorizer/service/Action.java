/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.authorizer.service;

import java.util.Objects;

/**
 * An action encapsulates an operation on a given resource identified by a name.
 * @param operation The operation (and the resource type)
 * @param resourceName The resource name
 */
public record Action(
                     ResourceType<?> operation,
                     String resourceName) {

    public Action {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(resourceName);
    }

    @SuppressWarnings({ "rawtypes", "unchecked", "java:S1452" })
    public Class<? extends ResourceType<?>> resourceTypeClass() {
        return (Class) operation.getClass();
    }

}
