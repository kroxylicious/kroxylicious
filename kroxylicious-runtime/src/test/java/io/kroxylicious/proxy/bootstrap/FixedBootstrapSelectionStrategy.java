/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kroxylicious.proxy.internal.util.Assertions;

/**
 * {@link BootstrapSelectionStrategy} that selects a fixed server from the given list of servers as the bootstrap server
 * based on the given choice.
 *
 * @param choice zero based index of the server to be selected.
 */
public record FixedBootstrapSelectionStrategy(int choice) implements BootstrapSelectionStrategy {

    @JsonCreator
    public FixedBootstrapSelectionStrategy {
        Assertions.requirePositive(choice, "bootstrap server choice");
    }

    @Override
    public String getStrategy() {
        return "fixed";
    }

    // the selector is stateless, so it never needs to be independent of any other
    @Override
    public BootstrapServerSelector newSelector() {
        return hostPorts -> {
            if (choice >= hostPorts.size()) {
                throw new IllegalStateException("Configured to use a server entry which is not available.");
            }
            return hostPorts.get(choice);
        };
    }
}
