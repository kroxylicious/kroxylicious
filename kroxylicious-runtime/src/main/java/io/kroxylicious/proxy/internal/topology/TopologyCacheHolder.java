/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.topology;

import java.util.concurrent.atomic.AtomicReference;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Lazily creates and holds the single {@link TopologyCache} shared by all connections through one
 * router level - see {@link io.kroxylicious.proxy.topology.TopologyService}'s "cache scope"
 * documentation. Thread-safe: {@link #getOrCreate()} may be invoked concurrently by different
 * connections' event-loop threads racing to create a router for the same router level.
 */
public final class TopologyCacheHolder {

    private final AtomicReference<TopologyCache> cache = new AtomicReference<>();

    /**
     * Creates a holder with no cache created yet.
     */
    public TopologyCacheHolder() {
        // Exists for Javadoc
    }

    /**
     * Returns the shared cache, creating it on first call.
     *
     * @return the shared cache
     */
    public TopologyCache getOrCreate() {
        return cache.updateAndGet(existing -> existing != null ? existing : new TopologyCache());
    }

    /**
     * Returns the shared cache if one has already been created, or {@code null} if
     * {@link #getOrCreate()} has never been called.
     *
     * @return the shared cache, or {@code null} if not yet created
     */
    @Nullable
    public TopologyCache getIfPresent() {
        return cache.get();
    }
}
