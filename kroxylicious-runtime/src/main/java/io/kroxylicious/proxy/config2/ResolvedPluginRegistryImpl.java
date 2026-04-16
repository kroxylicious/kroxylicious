/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;

/**
 * Implementation of {@link ResolvedPluginRegistry} built during config2 resolution.
 * Creates plugin instances via {@link PluginFactory} and stores them alongside
 * their resolved configurations for lookup by filters.
 */
class ResolvedPluginRegistryImpl implements ResolvedPluginRegistry {

    private record InstanceAndConfig(Object instance, Object config) {}

    /**
     * Keyed by "pluginInterfaceName/instanceName" for fast lookup.
     */
    private final Map<String, InstanceAndConfig> instances = new HashMap<>();

    /**
     * Builds a registry from the resolved plugin configs, creating instances
     * for non-filter plugins in dependency order.
     *
     * @param initOrder plugins in dependency order (dependencies before dependents)
     * @param pluginFactoryRegistry the registry for creating plugin instances
     * @param filterInterfaceName the FilterFactory interface name (instances are NOT pre-created)
     * @return the populated registry
     */
    static ResolvedPluginRegistryImpl build(
                                            List<ResolvedPluginConfig> initOrder,
                                            PluginFactoryRegistry pluginFactoryRegistry,
                                            String filterInterfaceName) {
        var registry = new ResolvedPluginRegistryImpl();
        for (ResolvedPluginConfig rpc : initOrder) {
            // Don't pre-create FilterFactory instances — the runtime manages those
            if (rpc.pluginInterfaceName().equals(filterInterfaceName)) {
                continue;
            }
            try {
                Class<?> pluginInterface = Class.forName(rpc.pluginInterfaceName());
                PluginFactory<?> factory = pluginFactoryRegistry.pluginFactory(pluginInterface);
                Object instance = factory.pluginInstance(rpc.pc().type());
                String key = key(rpc.pluginInterfaceName(), rpc.pluginInstanceName());
                registry.instances.put(key, new InstanceAndConfig(instance, rpc.pc().config()));
            }
            catch (ClassNotFoundException e) {
                throw new IllegalArgumentException(
                        "Plugin interface class not found: " + rpc.pluginInterfaceName(), e);
            }
        }
        return registry;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <P> P pluginInstance(
                                Class<P> pluginInterface,
                                String instanceName) {
        String key = key(pluginInterface.getName(), instanceName);
        InstanceAndConfig entry = instances.get(key);
        if (entry == null) {
            throw new UnknownPluginInstanceException(
                    "No resolved plugin instance '" + instanceName
                            + "' for interface '" + pluginInterface.getName() + "'");
        }
        return (P) entry.instance();
    }

    @Override
    public Object pluginConfig(
                               String pluginInterfaceName,
                               String instanceName) {
        String key = key(pluginInterfaceName, instanceName);
        InstanceAndConfig entry = instances.get(key);
        if (entry == null) {
            throw new UnknownPluginInstanceException(
                    "No resolved plugin instance '" + instanceName
                            + "' for interface '" + pluginInterfaceName + "'");
        }
        return entry.config();
    }

    private static String key(
                              String pluginInterfaceName,
                              String instanceName) {
        return pluginInterfaceName + "/" + instanceName;
    }
}
