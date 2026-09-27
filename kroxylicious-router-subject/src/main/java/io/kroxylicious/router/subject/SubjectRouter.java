/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;

/**
 * A {@link RouterFactory} that routes each client connection to a downstream cluster based on the
 * connection's authenticated identity, using a pluggable {@link RouteSelector}.
 */
@Plugin(configType = SubjectRouter.Config.class)
public class SubjectRouter implements RouterFactory<SubjectRouter.Config, SubjectRouter.Initialized> {

    /**
     * Creates a {@link SubjectRouter}.
     */
    public SubjectRouter() {
    }

    /**
     * Configuration for {@link SubjectRouter}.
     *
     * @param selector the name of the {@link RouteSelector} implementation used to select a route for a subject
     * @param selectorConfig the selector implementation's configuration
     */
    public record Config(@JsonProperty(required = true) @PluginImplName(RouteSelector.class) String selector,
                         @PluginImplConfig(implNameProperty = "selector") Object selectorConfig) {}

    /**
     * Immutable, thread-safe: shared across all connections of a virtual cluster.
     *
     * @param selector the initialized route selector
     * @param routeNames the route names declared for this router
     */
    public record Initialized(RouteSelector<Object> selector, Set<String> routeNames) {}

    @Override
    public Initialized initialize(RouterFactoryContext context, Config config) throws PluginConfigurationException {
        Config cfg = Plugins.requireConfig(this, config);
        RouteSelector<Object> selector = context.pluginInstance(RouteSelector.class, cfg.selector());
        selector.initialize(cfg.selectorConfig());
        Set<String> routeNames = context.routeNames();
        context.allowSharedClusterTargets(); // multiple identities may target the same cluster

        // Startup validation: every route the selector declares it may return must exist.
        // Empty Optional means the selector routes dynamically -> skip validation.
        // referencedRoutes() is sync-only, so resolving the stage here does not block on I/O.
        selector.referencedRoutes().toCompletableFuture().join().ifPresent(declared -> {
            Set<String> unknown = new HashSet<>(declared);
            unknown.removeAll(routeNames);
            if (!unknown.isEmpty()) {
                throw new PluginConfigurationException(
                        "selector references unknown routes " + unknown + "; declared routes are " + routeNames);
            }
        });
        return new Initialized(selector, routeNames);
    }

    @Override
    public Router createRouter(RouterFactoryContext context, Initialized init) {
        return new SubjectRoutingHandler(init.selector(), new RouteSelectorContextImpl(init.routeNames()),
                context.virtualClusterName(), context.routerName());
    }
}
