/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.resources;

import io.fabric8.kubernetes.api.model.HasMetadata;

public interface ResourceType<T extends HasMetadata> {
    String getKind();

    /**
     * Retrieve resource using Kubernetes API
     * @return specific resource with T type.
     */
    T get(String namespace, String name);

    /**
     * Creates specific resource based on T type using Kubernetes API
     */
    void create(T resource);

    /**
     * Delete specific resource based on T type using Kubernetes API
     */
    void delete(T resource);

    /**
     * Update specific resource based on T type using Kubernetes API
     */
    void update(T resource);

    /**
     * Check if this resource is marked as ready or not with wait.
     *
     * @return true if ready.
     */
    boolean waitForReadiness(T resource);
}
