/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Strategy for selecting an upstream target from a given list of upstream targets for bootstrapping.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = FixedBootstrapSelectionStrategy.class, property = "strategy")
@JsonSubTypes({
        @JsonSubTypes.Type(value = FixedBootstrapSelectionStrategy.class, name = "first"),
        @JsonSubTypes.Type(value = RandomBootstrapSelectionStrategy.class, name = "random")
})
public interface BootstrapSelectionStrategy {

    BootstrapSelectionStrategy FIRST_BOOTSTRAP_SERVER_SELECTION_STRATEGY = new FixedBootstrapSelectionStrategy(0);

    HostPort apply(List<HostPort> hostPorts);

    @SuppressWarnings("unused")
    @JsonSetter("strategy")
    default void setStrategy(String strategy) {
    }
}
