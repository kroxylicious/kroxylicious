/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.ThreadSafe;

/**
 * {@link BootstrapSelectionStrategy} that selects a server from the given list of servers as the bootstrap server in a round-robin fashion.
 * <p>
 * This class is immutable configuration. The round-robin position is held by the selector returned from
 * {@link #newSelector()}; each selector starts from the first server in the list and wraps around, so each
 * upstream cluster model begins its own cycle at the first server.
 */
public class RoundRobinBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    /**
     * Creates a round-robin bootstrap selection strategy.
     */
    public RoundRobinBootstrapSelectionStrategy() {
        // Intentionally empty
    }

    @Override
    public String getStrategy() {
        return "round-robin";
    }

    @Override
    public BootstrapServerSelector newSelector() {
        return new Selector();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // All RoundRobinBootstrapSelectionStrategy instances are equal: the strategy takes no parameters
        return o instanceof RoundRobinBootstrapSelectionStrategy;
    }

    @Override
    public int hashCode() {
        // All instances have same hash (type-based)
        return RoundRobinBootstrapSelectionStrategy.class.hashCode();
    }

    /**
     * Selector holding the round-robin position. The position is an atomic counter so that the
     * selector can be shared by concurrent connections without lost updates or an out-of-range index.
     */
    @ThreadSafe
    private static final class Selector implements BootstrapServerSelector {

        private final AtomicLong counter = new AtomicLong();

        @Override
        public HostPort select(List<HostPort> bootstrapServers) {
            long next = counter.getAndIncrement();
            return bootstrapServers.get(Math.floorMod(next, bootstrapServers.size()));
        }
    }
}
