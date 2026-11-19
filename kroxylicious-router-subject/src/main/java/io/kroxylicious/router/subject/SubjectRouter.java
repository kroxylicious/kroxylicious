/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

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
 * connection's authenticated identity, matched against an ordered list of rules.
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
     * @param routingRules ordered rules, evaluated in declaration order against the authenticated
     *                     subject and the connection's pinned {@code client.id}; the router uses the
     *                     route of the first matching rule. The last entry may instead be a bare
     *                     {@code otherwiseUseRoute} wildcard; see {@link Rule}.
     */
    public record Config(@JsonProperty(required = true) List<Rule> routingRules) {}

    /**
     * A single routing rule. Either a predicate rule, needing at least one of {@code hasPrincipals} or
     * {@code hasClientId} plus a required {@code useRoute}, or, only as {@code routingRules}' last
     * entry, a bare {@code otherwiseUseRoute}.
     *
     * @param hasPrincipals list of {@code {withType, withName}} pairs; the rule matches only if the
     *                      subject carries, for every listed pair, a principal of that type with that
     *                      name (checked via {@code Subject.allPrincipalsOfType(withType)}, a membership
     *                      check rather than a uniqueness check). A list rather than a map keyed by type,
     *                      since a rule may need two pairs with the same type and different names (for
     *                      example two {@code Role} principals). May be {@code null} or empty if
     *                      {@code hasClientId} is given.
     * @param hasClientId required value for the connection's pinned {@code client.id}; unlike
     *                    {@code hasPrincipals}, this can match before, or without, authentication,
     *                    since {@code client.id} is read from the request header rather than the
     *                    authenticated subject. A client-supplied, unverified value: see the module
     *                    README for what matching on it does and does not guarantee.
     * @param useRoute the route name, declared in the router's {@code routes} block, to forward to when
     *                 the rule matches. Required unless {@code otherwiseUseRoute} is given.
     * @param otherwiseUseRoute route for an authenticated subject that no preceding rule matches; a
     *                          wildcard {@code hasPrincipals} match (any principals, including none),
     *                          so it never matches an anonymous subject. Must not be combined with
     *                          {@code hasPrincipals}, {@code hasClientId}, or {@code useRoute}, and may
     *                          only appear as {@code routingRules}' last entry. If no rule configures
     *                          it, an authenticated subject matching no rule is rejected fail-closed.
     */
    public record Rule(@Nullable List<PrincipalRequirement> hasPrincipals,
                       @Nullable String hasClientId,
                       @Nullable String useRoute,
                       @Nullable String otherwiseUseRoute) {
        public Rule(@Nullable List<PrincipalRequirement> hasPrincipals, @Nullable String hasClientId, String useRoute) {
            this(hasPrincipals, hasClientId, useRoute, null);
        }
    }

    /**
     * A single {@code hasPrincipals} entry: the subject must carry a principal of {@code withType} named
     * {@code withName}.
     *
     * @param withType fully-qualified {@link Principal} implementation name
     * @param withName required principal name
     */
    public record PrincipalRequirement(@JsonProperty(required = true) String withType, @JsonProperty(required = true) String withName) {}

    /**
     * A {@link PrincipalRequirement} with {@code withType} resolved to a {@link Principal} class.
     *
     * @param type the principal type
     * @param name the required principal name
     */
    public record CompiledPrincipalRequirement(Class<? extends Principal> type, String name) {}

    /**
     * A {@link Rule} with its {@code hasPrincipals} types resolved to {@link Principal} classes.
     *
     * @param requiredPrincipals every pair the subject must satisfy for the rule to match
     * @param requiredClientId required value for the connection's pinned {@code client.id}, or
     *                         {@code null} if the rule does not match on it
     * @param route the route name to forward to when the rule matches
     */
    public record CompiledRule(List<CompiledPrincipalRequirement> requiredPrincipals, @Nullable String requiredClientId, String route) {}

    /**
     * Key used to detect two rules with identical predicates at startup. {@code hasPrincipals} is a
     * {@link Set} here, not a {@link List}: predicate identity is order-insensitive.
     */
    private record PredicateKey(Set<PrincipalRequirement> hasPrincipals, @Nullable String hasClientId) {}

    /**
     * Immutable, thread-safe: shared across all connections of a virtual cluster.
     *
     * @param rules ordered, compiled rules
     * @param defaultRoute the route for an authenticated subject that no rule matches, or {@code null}
     *                     to reject it
     */
    public record Initialized(List<CompiledRule> rules, @Nullable String defaultRoute) {}

    @Override
    public Initialized initialize(RouterFactoryContext context, Config config) throws PluginConfigurationException {
        Config cfg = Plugins.requireConfig(this, config);
        List<Rule> routingRules = cfg.routingRules();

        Map<String, Class<? extends Principal>> resolvedTypes = new HashMap<>();
        Set<PredicateKey> seenPredicates = new HashSet<>();
        List<CompiledRule> compiledRules = new ArrayList<>();
        String defaultRoute = null;
        for (int i = 0; i < routingRules.size(); i++) {
            Rule rule = routingRules.get(i);
            List<PrincipalRequirement> hasPrincipals = rule.hasPrincipals() == null ? List.of() : rule.hasPrincipals();
            if (rule.otherwiseUseRoute() != null) {
                if (!hasPrincipals.isEmpty() || rule.hasClientId() != null || rule.useRoute() != null) {
                    throw new PluginConfigurationException("routingRules otherwiseUseRoute cannot be combined with hasPrincipals, hasClientId, or useRoute");
                }
                if (i != routingRules.size() - 1) {
                    throw new PluginConfigurationException("routingRules otherwiseUseRoute must be the last entry");
                }
                defaultRoute = rule.otherwiseUseRoute();
                continue;
            }
            if (hasPrincipals.isEmpty() && rule.hasClientId() == null) {
                throw new PluginConfigurationException("routingRules entry needs at least one of hasPrincipals or hasClientId");
            }
            if (rule.useRoute() == null) {
                throw new PluginConfigurationException("routingRules entry needs useRoute");
            }
            if (!seenPredicates.add(new PredicateKey(Set.copyOf(hasPrincipals), rule.hasClientId()))) {
                throw new PluginConfigurationException(
                        "routingRules contains more than one rule with hasPrincipals " + hasPrincipals + " and hasClientId " + rule.hasClientId());
            }
            List<CompiledPrincipalRequirement> requiredPrincipals = new ArrayList<>();
            for (PrincipalRequirement requirement : hasPrincipals) {
                Class<? extends Principal> type = resolvedTypes.computeIfAbsent(requirement.withType(), SubjectRouter::resolvePrincipalType);
                requiredPrincipals.add(new CompiledPrincipalRequirement(type, requirement.withName()));
            }
            compiledRules.add(new CompiledRule(List.copyOf(requiredPrincipals), rule.hasClientId(), rule.useRoute()));
        }

        Set<String> routeNames = context.routeNames();
        Set<String> referenced = new HashSet<>();
        compiledRules.forEach(rule -> referenced.add(rule.route()));
        if (defaultRoute != null) {
            referenced.add(defaultRoute);
        }
        referenced.removeAll(routeNames);
        if (!referenced.isEmpty()) {
            throw new PluginConfigurationException(
                    "routingRules references unknown routes " + referenced + "; declared routes are " + routeNames);
        }

        context.allowSharedClusterTargets(); // multiple rules may target the same route
        return new Initialized(List.copyOf(compiledRules), defaultRoute);
    }

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "init is marked @Nullable to match the @UnknownNullness contract of RouterFactory#createRouter,"
            + " but the runtime only invokes this method with the value returned by initialize, which is never null.")
    @Override
    public Router createRouter(RouterFactoryContext context, @Nullable Initialized init) {
        Objects.requireNonNull(init);
        return new SubjectRoutingRouter(init.rules(), init.defaultRoute(), context.routeNames(),
                context.virtualClusterName(), context.routerName());
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Principal> resolvePrincipalType(String typeName) {
        Class<?> type;
        try {
            type = Class.forName(typeName);
        }
        catch (ClassNotFoundException e) {
            throw new PluginConfigurationException("routingRules hasPrincipals withType '" + typeName + "' not found", e);
        }
        if (!Principal.class.isAssignableFrom(type)) {
            throw new PluginConfigurationException("routingRules hasPrincipals withType '" + typeName + "' does not implement " + Principal.class.getName());
        }
        return (Class<? extends Principal>) type;
    }
}
