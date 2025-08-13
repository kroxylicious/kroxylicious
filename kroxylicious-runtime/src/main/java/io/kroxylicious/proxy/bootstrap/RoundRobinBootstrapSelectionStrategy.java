/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import io.kroxylicious.proxy.service.HostPort;

/**
 * {@link BootstrapSelectionStrategy} that selects a server from the given list of servers as the bootstrap server in a round-robin fashion.
 * <br>
 * Each instance has a counter which starts from <code>0</code> and rounds over, which means each server selection for each
 * {@link io.kroxylicious.proxy.config.VirtualCluster} will start from <code>0</code>.
 *
 */
public class RoundRobinBootstrapSelectionStrategy implements BootstrapSelectionStrategy {

    private long counter;

    public RoundRobinBootstrapSelectionStrategy() {
        this.counter = -1;
    }

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        int choice = (int) getNext(hostPorts.size());
        return hostPorts.get(choice);
    }

    private synchronized long getNext(long ceil) {
        this.counter++;
        if (counter >= ceil) {
            this.counter = 0;
        }
        return this.counter;
    }
}
