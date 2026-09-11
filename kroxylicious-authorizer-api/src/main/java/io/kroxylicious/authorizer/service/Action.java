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

    /**
     * Creates an action.
     * @param operation The operation (and the resource type)
     * @param resourceName The resource name
     * @throws NullPointerException if the {@code operation} or {@code resourceName} is null
     */
    public Action {
        Objects.requireNonNull(operation);
        Objects.requireNonNull(resourceName);
    }

    /**
     * The class of the {@linkplain #operation() operation}, which identifies the type of resource being acted upon.
     * @return The class of the operation.
     */
    @SuppressWarnings({ "rawtypes", "unchecked", "java:S1452" })
    public Class<? extends ResourceType<?>> resourceTypeClass() {
        return (Class) operation.getClass();
    }

    @Override
    public String toString() {
        return "Action[" +
                "operation=" + operation.getClass().getSimpleName() + '.' + operation +
                ", resourceName='" + resourceName + '\'' +
                ']';
    }
}
