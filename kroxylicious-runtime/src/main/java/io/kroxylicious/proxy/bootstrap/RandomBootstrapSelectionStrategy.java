/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Strategy which chooses a random upstream target as the bootstrap server.
 */
public class RandomBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        final int choice = (int) (Math.random() * hostPorts.size());
        return hostPorts.get(choice);
    }
}
