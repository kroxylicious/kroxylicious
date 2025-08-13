/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kroxylicious.proxy.internal.util.Assertions;
import io.kroxylicious.proxy.service.HostPort;

public record FixedBootstrapSelectionStrategy(int choice) implements BootstrapSelectionStrategy {

    @JsonCreator
    public FixedBootstrapSelectionStrategy {
        Assertions.requirePositive(choice, "bootstrap server choice");
    }

    @Override
    public HostPort apply(List<HostPort> hostPorts) {
        return hostPorts.get(choice);
    }
}
