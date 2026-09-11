/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import java.util.List;
import java.util.function.Function;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kroxylicious.proxy.service.HostPort;

/**
 * Strategy for selecting an upstream target from a given list of upstream targets for bootstrapping.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = RoundRobinBootstrapSelectionStrategy.class, property = "strategy", include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RandomBootstrapSelectionStrategy.class, name = "random"),
        @JsonSubTypes.Type(value = RoundRobinBootstrapSelectionStrategy.class, name = "round-robin")
})
public interface BootstrapSelectionStrategy extends Function<List<HostPort>, HostPort> {

    @Override
    @JsonIgnore
    HostPort apply(List<HostPort> hostPorts);

    /**
     * No-op setter that allows the {@code strategy} discriminator property to be present in the
     * configuration YAML; the actual strategy selection is performed by Jackson polymorphic
     * deserialization.
     *
     * @param strategy the strategy name from the configuration; ignored
     */
    @SuppressWarnings("unused")
    @JsonSetter("strategy")
    default void setStrategy(String strategy) {
    }

    /**
     * Returns the name identifying this strategy (e.g. {@code random} or {@code round-robin}),
     * used as the {@code strategy} discriminator property when serializing the configuration.
     *
     * @return the strategy name
     */
    @JsonGetter("strategy")
    String getStrategy();
}
