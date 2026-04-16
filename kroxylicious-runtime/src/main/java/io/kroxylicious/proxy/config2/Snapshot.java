/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.List;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A point-in-time view of the entire proxy configuration, including the proxy's own
 * configuration and all plugin instance configurations. Abstracts the configuration
 * source (filesystem, ConfigMap, database, etc.) from the parser.
 *
 * <p>Each plugin instance has metadata (name, type, version, generation) and data bytes.
 * For most plugins, the data bytes are UTF-8 YAML. For binary resource plugins (those
 * annotated with {@code @ResourceType}), the data bytes are the raw resource content
 * (e.g. a PKCS12 keystore).</p>
 */
public interface Snapshot {

    /**
     * Returns the proxy configuration YAML content.
     *
     * @return the proxy configuration as a YAML string
     */
    String proxyConfig();

    /**
     * Returns the plugin interface names for which plugin instances are configured.
     *
     * @return list of plugin interface names (fully qualified class names)
     */
    List<String> pluginInterfaces();

    /**
     * Returns the names of all plugin instances configured for a given plugin interface.
     *
     * @param pluginInterfaceName the fully qualified name of the plugin interface
     * @return list of plugin instance names
     */
    List<String> pluginInstances(String pluginInterfaceName);

    /**
     * Returns the metadata and data bytes for a specific plugin instance as an atomic unit.
     * For YAML-based plugins, the data is the UTF-8 encoded YAML content (including the
     * metadata envelope). For binary resource plugins (annotated with {@code @ResourceType}),
     * the data is the raw resource bytes.
     *
     * <p>Callers must not modify the returned data array.</p>
     *
     * @param pluginInterfaceName the fully qualified name of the plugin interface
     * @param pluginInstanceName the name of the plugin instance
     * @return the plugin instance content (metadata and data)
     * @throws IllegalArgumentException if the plugin instance is not found
     */
    PluginInstanceContent pluginInstance(
                                         String pluginInterfaceName,
                                         String pluginInstanceName);

    /**
     * Returns the password for a named resource, if one is configured. Passwords are
     * provided out-of-band from the resource data for security.
     *
     * @param resourceName the name of the resource
     * @return the password as a char array, or {@code null} if no password is configured
     */
    @Nullable
    char[] resourcePassword(String resourceName);
}
