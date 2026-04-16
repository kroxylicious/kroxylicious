/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

/**
 * Metadata for a plugin instance within a {@link Snapshot}.
 *
 * @param name the instance name
 * @param type the fully qualified class name of the plugin implementation
 * @param version the configuration version
 * @param shared whether this instance is shared across consumers
 * @param generation a monotonically increasing number that changes when the resource content changes
 */
public record PluginInstanceMetadata(
                                     String name,
                                     String type,
                                     String version,
                                     boolean shared,
                                     long generation) {}
