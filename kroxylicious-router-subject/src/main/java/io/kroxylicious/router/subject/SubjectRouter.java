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
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kroxylicious.identity.SingularPrincipals;
import io.kroxylicious.proxy.authentication.Principal;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link RouterFactory} that routes each client connection to a downstream cluster based on the
 * connection's authenticated identity, matched against a static mapping of principal name to route.
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
     * @param singularPrincipalMatch the principal-to-route matching configuration
     */
    public record Config(@JsonProperty(required = true) SingularPrincipalMatch singularPrincipalMatch) {}

    /**
     * Maps the subject's unique principal of {@code type} to a route via a static {@code routeMapping}.
     *
     * @param type fully-qualified name of a {@link Principal} implementation annotated as singular
     *             ({@code @Unique} or {@code @SingularPrincipal}); the factory refuses to start if
     *             the type is not singular
     * @param routeMapping principal-name-to-route entries; a principal name must not appear more than once
     * @param defaultRoute route for an authenticated subject whose principal name is absent from
     *                     {@code routeMapping}, or that carries no principal of {@code type} at all;
     *                     if {@code null}, such a subject is rejected fail-closed
     */
    public record SingularPrincipalMatch(@JsonProperty(required = true) String type,
                                         @JsonProperty(required = true) List<RouteMappingEntry> routeMapping,
                                         @Nullable String defaultRoute) {}

    /**
     * Pairs a principal name with the route its subject should be forwarded to.
     *
     * @param principalName the principal name
     * @param route the route name, declared in the router's {@code routes} block
     */
    public record RouteMappingEntry(@JsonProperty(required = true) String principalName,
                                    @JsonProperty(required = true) String route) {}

    /**
     * Immutable, thread-safe: shared across all connections of a virtual cluster.
     *
     * @param principalType the singular principal type matched against the authenticated subject
     * @param principalToRoute the principal-name-to-route mapping
     * @param defaultRoute the route for an unmapped subject, or {@code null} to reject it
     */
    public record Initialized(Class<? extends Principal> principalType, Map<String, String> principalToRoute, @Nullable String defaultRoute) {}

    @Override
    public Initialized initialize(RouterFactoryContext context, Config config) throws PluginConfigurationException {
        Config cfg = Plugins.requireConfig(this, config);
        SingularPrincipalMatch match = cfg.singularPrincipalMatch();
        Class<? extends Principal> principalType = resolvePrincipalType(match.type());
        if (!SingularPrincipals.isSingular(principalType)) {
            throw new PluginConfigurationException(
                    "singularPrincipalMatch.type " + principalType.getName() + " is not a singular principal type");
        }

        Map<String, String> principalToRoute = new HashMap<>();
        for (RouteMappingEntry entry : match.routeMapping()) {
            if (principalToRoute.putIfAbsent(entry.principalName(), entry.route()) != null) {
                throw new PluginConfigurationException(
                        "singularPrincipalMatch.routeMapping contains principal '" + entry.principalName() + "' more than once");
            }
        }

        Set<String> routeNames = context.routeNames();
        Set<String> referenced = new HashSet<>(principalToRoute.values());
        if (match.defaultRoute() != null) {
            referenced.add(match.defaultRoute());
        }
        referenced.removeAll(routeNames);
        if (!referenced.isEmpty()) {
            throw new PluginConfigurationException(
                    "singularPrincipalMatch references unknown routes " + referenced + "; declared routes are " + routeNames);
        }

        context.allowSharedClusterTargets(); // multiple identities may target the same cluster
        return new Initialized(principalType, Map.copyOf(principalToRoute), match.defaultRoute());
    }

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "init is marked @Nullable to match the @UnknownNullness contract of RouterFactory#createRouter,"
            + " but the runtime only invokes this method with the value returned by initialize, which is never null.")
    @Override
    public Router createRouter(RouterFactoryContext context, @Nullable Initialized init) {
        Objects.requireNonNull(init);
        return new SubjectRoutingHandler(init.principalType(), init.principalToRoute(), init.defaultRoute(), context.routeNames(),
                context.virtualClusterName(), context.routerName());
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Principal> resolvePrincipalType(String typeName) {
        Class<?> type;
        try {
            type = Class.forName(typeName);
        }
        catch (ClassNotFoundException e) {
            throw new PluginConfigurationException("singularPrincipalMatch.type '" + typeName + "' not found", e);
        }
        if (!Principal.class.isAssignableFrom(type)) {
            throw new PluginConfigurationException("singularPrincipalMatch.type '" + typeName + "' does not implement " + Principal.class.getName());
        }
        return (Class<? extends Principal>) type;
    }
}
