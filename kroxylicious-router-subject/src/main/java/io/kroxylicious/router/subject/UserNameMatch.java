/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.proxy.authentication.Subject;
import io.kroxylicious.proxy.authentication.User;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link RouteSelector} that maps Kafka principal names to route names.
 */
@Plugin(configType = UserNameMatch.Config.class)
public class UserNameMatch implements RouteSelector<UserNameMatch.Config> {

    /**
     * Maps a route to the principals that should be sent to it.
     *
     * @param route the route name
     * @param principals the principal names mapped to {@code route}
     */
    public record Mapping(@JsonProperty(required = true) String route,
                          @JsonProperty(required = true) List<String> principals) {}

    /**
     * Configuration for {@link UserNameMatch}.
     *
     * @param mappings the principal-to-route mappings
     * @param defaultRoute the route used for principals not present in any mapping, or {@code null} to reject them
     */
    public record Config(@JsonProperty(required = true) List<Mapping> mappings,
                         @Nullable String defaultRoute) {}

    private Map<String, String> principalToRoute = Map.of();
    private @Nullable String defaultRoute;
    private Set<String> referencedRoutes = Set.of();

    /**
     * Creates a new instance, invoked by the plugin framework. State is populated by {@link #initialize(Config)}.
     */
    public UserNameMatch() {
    }

    @Override
    public void initialize(Config config) {
        Config cfg = Plugins.requireConfig(this, config);
        Map<String, String> mapping = new HashMap<>();
        for (Mapping m : cfg.mappings()) {
            for (String principal : m.principals()) {
                String existing = mapping.putIfAbsent(principal, m.route());
                if (existing != null && !existing.equals(m.route())) {
                    throw new PluginConfigurationException(
                            "principal '" + principal + "' is mapped to multiple routes: '" + existing + "' and '" + m.route() + "'");
                }
            }
        }
        Set<String> routes = new HashSet<>();
        cfg.mappings().forEach(m -> routes.add(m.route()));
        if (cfg.defaultRoute() != null) {
            routes.add(cfg.defaultRoute());
        }
        this.principalToRoute = Map.copyOf(mapping);
        this.defaultRoute = cfg.defaultRoute();
        this.referencedRoutes = Set.copyOf(routes);
    }

    @Override
    @SuppressWarnings({ "java:S5738", "removal" })
    public CompletionStage<Optional<String>> selectRoute(Subject subject, RouteSelectorContext context) {
        if (subject.isAnonymous()) {
            return CompletableFuture.completedFuture(Optional.empty());
        }
        Optional<String> route = subject.uniquePrincipalOfType(User.class)
                .map(User::name)
                .map(principalToRoute::get)
                .or(() -> Optional.ofNullable(defaultRoute));
        return CompletableFuture.completedFuture(route);
    }

    @Override
    public CompletionStage<Optional<Set<String>>> referencedRoutes() {
        return CompletableFuture.completedFuture(Optional.of(referencedRoutes));
    }
}
