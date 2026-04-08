/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.lifecycle;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Thread-safe registry of virtual cluster lifecycles. One instance per proxy.
 */
public class VirtualClusterLifecycleManager {

    private final ConcurrentHashMap<String, VirtualClusterLifecycle> lifecycles = new ConcurrentHashMap<>();

    /**
     * Registers a new virtual cluster lifecycle in the {@link VirtualClusterLifecycleState#INITIALIZING} state.
     *
     * @param clusterName the virtual cluster name
     * @return the newly created lifecycle
     * @throws IllegalArgumentException if a lifecycle for this cluster name already exists
     */
    public VirtualClusterLifecycle register(String clusterName) {
        var lifecycle = new VirtualClusterLifecycle(clusterName);
        var existing = lifecycles.putIfAbsent(clusterName, lifecycle);
        if (existing != null) {
            throw new IllegalArgumentException("Virtual cluster '%s' is already registered".formatted(clusterName));
        }
        return lifecycle;
    }

    /**
     * Returns the lifecycle for the given cluster name, or {@code null} if not registered.
     */
    @Nullable
    public VirtualClusterLifecycle get(String clusterName) {
        return lifecycles.get(clusterName);
    }

    /**
     * Returns {@code true} if the named virtual cluster is in the
     * {@link VirtualClusterLifecycleState#SERVING} state. Returns {@code false}
     * if not registered or in any other state.
     */
    public boolean isAcceptingConnections(String clusterName) {
        var lifecycle = lifecycles.get(clusterName);
        return lifecycle != null && lifecycle.isAcceptingConnections();
    }

    /**
     * Returns an unmodifiable view of all registered lifecycles.
     */
    public Collection<VirtualClusterLifecycle> all() {
        return Collections.unmodifiableCollection(lifecycles.values());
    }
}
