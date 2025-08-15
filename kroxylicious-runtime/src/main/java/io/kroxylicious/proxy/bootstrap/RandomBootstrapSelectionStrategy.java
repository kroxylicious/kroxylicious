/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import io.kroxylicious.proxy.service.HostPort;

/**
 * {@link BootstrapSelectionStrategy} which selects a random server from the given list of servers as the bootstrap server.
 */
public class RandomBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        final int choice = (int) (Math.random() * hostPorts.size());
        return hostPorts.get(choice);
    }

    @Override
    public String getStrategy() {
        return "random";
    }
}
