/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

import java.util.stream.Stream;

/**
 * Implemented by new-style (versioned) plugin configuration types to declare their
 * dependencies on other plugin instances. The framework calls {@link #pluginReferences()}
 * to discover cross-file dependencies, build a dependency graph, and validate that
 * all referenced plugin instances exist.
 */
public interface HasPluginReferences {

    /**
     * Returns all plugin references in this configuration, including references
     * from nested structures. Implementations must include all references to
     * ensure correct dependency tracking and initialisation ordering.
     *
     * @return a stream of all plugin references declared by this configuration
     */
    Stream<PluginReference<?>> pluginReferences();
}
