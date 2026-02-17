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

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        final int choice = random.nextInt(hostPorts.size());
        return hostPorts.get(choice);
    }

    @Override
    public String getStrategy() {
        return "random";
    }
}
