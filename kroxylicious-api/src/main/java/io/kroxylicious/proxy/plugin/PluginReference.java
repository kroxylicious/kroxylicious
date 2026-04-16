/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * A typed reference to a named plugin instance. Used by the framework to
 * build the dependency graph and determine initialisation order.
 * <p>
 * This is a runtime type, not a serialisation type. Plugin authors choose their
 * own YAML representation for references (e.g. a bare instance name string when
 * the plugin interface type is statically known) and construct {@code PluginReference}
 * instances in their {@link HasPluginReferences#pluginReferences()} implementation.
 *
 * @param type the fully qualified name of the plugin interface (e.g. {@code io.kroxylicious.kms.service.KmsService})
 * @param name the name of the plugin instance (e.g. {@code aws-kms})
 * @param <T> the plugin interface type
 */
public record PluginReference<T>(
                                 String type,
                                 String name) {}
