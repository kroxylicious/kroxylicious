/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;

/**
 * Helper that detects whether two {@link Configuration} instances differ in any
 * <em>static</em> configuration section &mdash; i.e. a section whose values are fixed at
 * proxy startup and cannot be reconciled at runtime.
 * <p>
 * Used by {@link ConfigurationReloadOrchestrator} as a pre-flight check before any
 * virtual-cluster change is attempted. If this differ reports a non-empty set, the
 * orchestrator rejects the reconfigure with {@code StaticConfigurationChangedException}.
 *
 * <p>The implementation enumerates the {@link Configuration} record's components reflectively
 * and treats every component as static <em>unless</em> it appears in {@link #RECONCILABLE}.
 * This makes the safe default automatic: a new field added to {@code Configuration} is
 * implicitly treated as static and any change to it is rejected at reconfigure time. To opt
 * a new field into runtime reconciliation, add its name to {@link #RECONCILABLE} <em>and</em>
 * implement a corresponding {@link ChangeDetector} in {@link ConfigurationReloadOrchestrator}.
 */
public class StaticSectionDiffer {

    /**
     * Names of {@link Configuration} record components the orchestrator can reconcile at
     * runtime. Every other component is treated as static.
     */
    private static final Set<String> RECONCILABLE = Set.of(
            "filterDefinitions",
            "defaultFilters",
            "virtualClusters");

    /**
     * Returns the names of static configuration sections that differ between {@code oldConfig}
     * and {@code newConfig}. An empty set indicates the two configurations agree on every
     * static section; the orchestrator may proceed to reconcile the reconcilable sections.
     * The returned set is immutable and its iteration order is unspecified.
     *
     * @param oldConfig the currently-running configuration
     * @param newConfig the submitted configuration
     * @return the names of sections that differ; never null, may be empty
     */
    public Set<String> diff(Configuration oldConfig, Configuration newConfig) {
        Objects.requireNonNull(oldConfig, "oldConfig");
        Objects.requireNonNull(newConfig, "newConfig");

        return Arrays.stream(Configuration.class.getRecordComponents())
                .filter(this::isStaticConfigSection)
                .filter(component -> hasChanged(component, oldConfig, newConfig))
                .map(RecordComponent::getName)
                .collect(Collectors.toUnmodifiableSet());
    }

    private boolean isStaticConfigSection(RecordComponent component) {
        return !RECONCILABLE.contains(component.getName());
    }

    private boolean hasChanged(RecordComponent component, Configuration oldConfig, Configuration newConfig) {
        return !Objects.equals(read(component, oldConfig), read(component, newConfig));
    }

    private static Object read(RecordComponent component, Configuration config) {
        try {
            return component.getAccessor().invoke(config);
        }
        catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException(
                    "Failed to read Configuration record component '" + component.getName() + "'", e);
        }
    }
}
