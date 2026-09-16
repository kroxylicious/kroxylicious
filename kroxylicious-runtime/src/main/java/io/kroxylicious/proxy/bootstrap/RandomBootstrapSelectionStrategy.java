/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.Random;

import com.fasterxml.jackson.annotation.JsonIgnore;

import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * {@link BootstrapSelectionStrategy} which selects a random server from the given list of servers as the bootstrap server.
 */
@SuppressFBWarnings("PREDICTABLE_RANDOM") // Pseudorandomness sufficient for port collision avoidance; not security relevant
public class RandomBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    @SuppressWarnings("java:S2245") // using insecure random is entirely appropriate here.

    @JsonIgnore
    private final Random random = new Random();

    /**
     * Creates a random bootstrap selection strategy.
     */
    public RandomBootstrapSelectionStrategy() {
        // Intentionally empty
    }

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        final int choice = random.nextInt(hostPorts.size());
        return hostPorts.get(choice);
    }

    @Override
    public String getStrategy() {
        return "random";
    }

    // a fresh Random too, so that virtual clusters don't contend on one instance's seed
    @Override
    public BootstrapSelectionStrategy newInstance() {
        return new RandomBootstrapSelectionStrategy();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // All RandomBootstrapSelectionStrategy instances are equal
        // (Random instance is runtime state, not configuration)
        return o instanceof RandomBootstrapSelectionStrategy;
    }

    @Override
    public int hashCode() {
        // All instances have same hash (type-based)
        return RandomBootstrapSelectionStrategy.class.hashCode();
    }
}
