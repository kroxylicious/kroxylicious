/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.plugin;

/**
 * Registry of plugin instances resolved during config2 processing. Provides
 * lookup of pre-created plugin instances and their configurations by plugin
 * interface and instance name.
 *
 * <p>Filters with versioned configs use this to obtain their dependencies
 * (e.g. an {@code AuthorizerService} or {@code KmsService}) rather than
 * creating them inline via {@code @PluginImplName}/{@code @PluginImplConfig}.
 */
public interface ResolvedPluginRegistry {

    /**
     * Returns a plugin instance for the given interface and instance name.
     * The instance is pre-created but not pre-initialised; the caller is
     * responsible for type-specific initialisation.
     *
     * @param pluginInterface the plugin interface class
     * @param instanceName the plugin instance name
     * @param <P> the plugin interface type
     * @return the plugin instance
     * @throws UnknownPluginInstanceException if no instance with that name exists
     */
    <P> P pluginInstance(
                         Class<P> pluginInterface,
                         String instanceName);

    /**
     * Returns the resolved configuration object for the given plugin instance.
     *
     * @param pluginInterfaceName the fully qualified plugin interface name
     * @param instanceName the plugin instance name
     * @return the configuration object (may be {@code null} if the plugin has no config)
     * @throws UnknownPluginInstanceException if no instance with that name exists
     */
    Object pluginConfig(
                        String pluginInterfaceName,
                        String instanceName);
}
