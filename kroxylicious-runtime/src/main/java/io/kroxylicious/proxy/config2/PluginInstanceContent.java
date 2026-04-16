/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

/**
 * The metadata and data bytes for a single plugin instance, obtained atomically
 * from a {@link Snapshot}.
 *
 * <p>Callers must not modify the returned {@code data} array.</p>
 *
 * @param metadata the instance metadata (name, type, version, generation)
 * @param data the raw data bytes (UTF-8 YAML or binary resource)
 */
public record PluginInstanceContent(
                                    PluginInstanceMetadata metadata,
                                    byte[] data) {}
