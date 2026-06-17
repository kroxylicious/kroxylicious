/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.router;

import java.util.Set;

import io.kroxylicious.proxy.plugin.UnknownPluginInstanceException;
import io.kroxylicious.proxy.topology.TopologyService;

/**
 * Context for {@link RouterFactory} initialisation and router creation,
 * providing access to the plugin registry and router identity.
 */
public interface RouterFactoryContext {

    /**
     * Returns the name of the virtual cluster that owns this router.
     *
     * @return the virtual cluster name
     */
    String virtualClusterName();

    /**
     * Returns the name of this router, as declared in the router definition.
     *
     * @return the router name
     */
    String routerName();

    /**
     * Gets a plugin instance for the given plugin type and name.
     *
     * @param <P> the plugin type
     * @param pluginClass the plugin class
     * @param implementationName the plugin implementation name
     * @return the plugin instance
     * @throws UnknownPluginInstanceException if the named implementation is unknown
     */
    <P> P pluginInstance(Class<P> pluginClass, String implementationName);

    /**
     * Returns the names of the routes configured for this router.
     * These are the route names declared in the router definition,
     * not route names that appear in the router's plugin configuration.
     *
     * @return an unmodifiable set of route names
     */
    Set<String> routeNames();

    /**
     * Returns the implementation names of the registered instances
     * of the given plugin type.
     *
     * @param <P> the plugin type
     * @param pluginClass the plugin class
     * @return the set of known implementation names
     */
    <P> Set<String> pluginImplementationNames(Class<P> pluginClass);

    /**
     * Returns a {@link TopologyService} for this router level.
     *
     * <p>The runtime creates the underlying topology cache on first
     * call and populates it from METADATA responses flowing through
     * the routing pipeline. If no router at a level calls this method,
     * no cache is created and no cost is incurred.</p>
     *
     * <p>When called during {@link RouterFactory#initialize}, this
     * triggers cache creation as an opt-in side effect. The returned
     * instance should <em>not</em> be stored in the factory's
     * initialization data.</p>
     *
     * <p>When called during {@link RouterFactory#createRouter}, this
     * returns a fresh per-connection instance backed by the shared
     * cache. This is the instance the router should use for discovery
     * methods ({@link TopologyService#leaders},
     * {@link TopologyService#coordinators}, etc.).</p>
     *
     * @return the topology service for this router level
     */
    TopologyService topologyService();

    /**
     * Declares that this router supports routes targeting the same
     * cluster.
     *
     * <p>By default, the runtime rejects configurations where two
     * routes in the same router resolve to the same cluster (directly
     * or transitively via nested routers), because most routers assume
     * each route is a distinct destination and will produce incorrect
     * results otherwise. Routers that handle this call this method
     * during {@link RouterFactory#initialize} to suppress the
     * check.</p>
     */
    void allowSharedClusterTargets();
}
