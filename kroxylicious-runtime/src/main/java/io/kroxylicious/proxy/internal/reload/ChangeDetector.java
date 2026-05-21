/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

/**
 * Identifies a category of configuration change between an old and a new {@link io.kroxylicious.proxy.config.Configuration}.
 * <p>
 * The orchestrator runs a pipeline of detectors over the same {@link ConfigurationChangeContext}
 * and merges their {@link ChangeResult} outputs via {@link ChangeResult#merge(ChangeResult)}.
 * Each detector focuses on one concern (virtual-cluster topology, filter definitions, etc.) so
 * individual diffs are easy to understand, test, and evolve.
 */
public interface ChangeDetector {

    /**
     * Examines the old and new configuration carried by {@code context} and returns the set of
     * virtual clusters that this detector believes should be added, removed, or modified as a
     * consequence of the change.
     */
    ChangeResult detect(ConfigurationChangeContext context);
}
