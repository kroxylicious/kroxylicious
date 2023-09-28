/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

/**
 * The interface Resource type.
 *
 * @param <T>  the type parameter
 */
public interface ResourceType<T extends HasMetadata> {
    /**
     * Gets kind.
     *
     * @return the kind
     */
    String getKind();

    /**
     * Retrieve resource using Kubernetes API
     * @param namespace the namespace
     * @param name the name
     * @return specific resource with T type.
     */
    T get(String namespace, String name);

    /**
     * Creates specific resource based on T type using Kubernetes API
     * @param resource the resource
     */
    void create(T resource);

    /**
     * Delete specific resource based on T type using Kubernetes API
     * @param resource the resource
     */
    void delete(T resource);

    /**
     * Update specific resource based on T type using Kubernetes API
     * @param resource the resource
     */
    void update(T resource);

    /**
     * Check if this resource is marked as ready or not with wait.
     *
     * @param resource the resource
     * @return true if ready.
     */
    boolean waitForReadiness(T resource);
}
