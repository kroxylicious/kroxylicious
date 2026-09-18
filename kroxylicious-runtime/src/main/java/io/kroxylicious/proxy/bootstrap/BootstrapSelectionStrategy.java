/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.bootstrap;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Configuration of the strategy used to select an upstream bootstrap server from the configured list
 * when a new upstream connection is made.
 * <p>
 * Implementations are immutable value objects describing <em>which</em> strategy is configured (and any
 * parameters it takes); they hold no selection state, and implement {@link Object#equals(Object)} and
 * {@link Object#hashCode()} in terms of that configuration alone so that re-parsing an unchanged
 * configuration yields an equal strategy. The mutable, per-upstream-cluster selection state lives in the
 * {@link BootstrapServerSelector} obtained from {@link #newSelector()}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = RoundRobinBootstrapSelectionStrategy.class, property = "strategy", include = JsonTypeInfo.As.EXISTING_PROPERTY)
@JsonSubTypes({
        @JsonSubTypes.Type(value = RandomBootstrapSelectionStrategy.class, name = "random"),
        @JsonSubTypes.Type(value = RoundRobinBootstrapSelectionStrategy.class, name = "round-robin")
})
public interface BootstrapSelectionStrategy {

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

    /**
     * Creates a new, thread-safe selector implementing this strategy.
     * <p>
     * Each call returns a selector whose selection state is independent of any other selector,
     * so callers that must not share selection state (for example, distinct upstream cluster
     * models) should each obtain their own.
     *
     * @return a new selector for this strategy
     */
    @JsonIgnore
    BootstrapServerSelector newSelector();
}
