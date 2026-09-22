/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import io.kroxylicious.proxy.service.HostPort;
import io.kroxylicious.proxy.tag.ThreadSafe;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * {@link BootstrapSelectionStrategy} which selects a random server from the given list of servers as the bootstrap server.
 * <p>
 * This class is immutable configuration; the selector returned from {@link #newSelector()} draws from the
 * calling thread's {@link ThreadLocalRandom}, so concurrent connections neither share nor contend on a seed.
 */
public class RandomBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    /**
     * Creates a random bootstrap selection strategy.
     */
    public RandomBootstrapSelectionStrategy() {
        // Intentionally empty
    }

    @Override
    public String getStrategy() {
        return "random";
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
        // All RandomBootstrapSelectionStrategy instances are equal: the strategy takes no parameters
        return o instanceof RandomBootstrapSelectionStrategy;
    }

    @Override
    public int hashCode() {
        // All instances have same hash (type-based)
        return RandomBootstrapSelectionStrategy.class.hashCode();
    }

    /**
     * Stateless selector; {@link ThreadLocalRandom} is used so that the selector is safe to share
     * between connections without contention.
     */
    @ThreadSafe
    @SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness sufficient for spreading bootstrap connections; not security relevant
    @SuppressWarnings("java:S2245") // using insecure random is entirely appropriate here.
    private static final class Selector implements BootstrapServerSelector {

        @Override
        public HostPort select(List<HostPort> bootstrapServers) {
            return bootstrapServers.get(ThreadLocalRandom.current().nextInt(bootstrapServers.size()));
        }
    }
}
