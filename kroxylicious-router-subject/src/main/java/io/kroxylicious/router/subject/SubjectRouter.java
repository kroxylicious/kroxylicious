/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.subject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
     * @param ruleMatch the ordered rule-based matching configuration
     */
    public record Config(@JsonProperty(required = true) RuleMatch ruleMatch) {}

    /**
     * Evaluates {@code rules} in declaration order against the authenticated subject, using the route
     * of the first matching rule.
     *
     * @param rules ordered rules; the first rule whose {@code hasPrincipals} is fully satisfied by the
     *              subject wins
     * @param defaultRoute route for an authenticated subject that no rule matches; if {@code null},
     *                     such a subject is rejected fail-closed. Never applies to an anonymous subject.
     */
    public record RuleMatch(@JsonProperty(required = true) List<Rule> rules, @Nullable String defaultRoute) {}

    /**
     * A single routing rule.
     *
     * @param hasPrincipals map of fully-qualified {@link Principal} implementation name to required
     *                      principal name; the rule matches only if the subject carries a principal of
     *                      every listed type, with the exact name given for each. Each referenced type
     *                      must be annotated {@code @Unique} or {@code @SingularPrincipal} (directly, or
     *                      via meta-annotation); the factory checks this reflectively in
     *                      {@code initialize()} and refuses to start otherwise.
     * @param useRoute the route name, declared in the router's {@code routes} block, to forward to when
     *                 the rule matches
     */
    public record Rule(@JsonProperty(required = true) Map<String, String> hasPrincipals,
                       @JsonProperty(required = true) String useRoute) {}

    /**
     * A {@link Rule} with its {@code hasPrincipals} types resolved to {@link Principal} classes.
     *
     * @param requiredPrincipals principal type to required principal name; the subject must carry a
     *                           principal of every listed type, with the exact name given, for the rule
     *                           to match
     * @param route the route name to forward to when the rule matches
     */
    public record CompiledRule(Map<Class<? extends Principal>, String> requiredPrincipals, String route) {}

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
        RuleMatch ruleMatch = cfg.ruleMatch();

        Map<String, Class<? extends Principal>> resolvedTypes = new HashMap<>();
        Set<Map<String, String>> seenPredicates = new HashSet<>();
        List<CompiledRule> compiledRules = new ArrayList<>();
        for (Rule rule : ruleMatch.rules()) {
            if (rule.hasPrincipals() == null || rule.hasPrincipals().isEmpty()) {
                throw new PluginConfigurationException("ruleMatch.rules entry needs a non-empty hasPrincipals");
            }
            if (!seenPredicates.add(rule.hasPrincipals())) {
                throw new PluginConfigurationException("ruleMatch.rules contains more than one rule with hasPrincipals " + rule.hasPrincipals());
            }
            Map<Class<? extends Principal>, String> requiredPrincipals = new LinkedHashMap<>();
            for (Map.Entry<String, String> entry : rule.hasPrincipals().entrySet()) {
                Class<? extends Principal> type = resolvedTypes.computeIfAbsent(entry.getKey(), SubjectRouter::resolvePrincipalType);
                requiredPrincipals.put(type, entry.getValue());
            }
            compiledRules.add(new CompiledRule(Map.copyOf(requiredPrincipals), rule.useRoute()));
        }

        Set<String> routeNames = context.routeNames();
        Set<String> referenced = new HashSet<>();
        compiledRules.forEach(rule -> referenced.add(rule.route()));
        if (ruleMatch.defaultRoute() != null) {
            referenced.add(ruleMatch.defaultRoute());
        }
        referenced.removeAll(routeNames);
        if (!referenced.isEmpty()) {
            throw new PluginConfigurationException(
                    "ruleMatch references unknown routes " + referenced + "; declared routes are " + routeNames);
        }

        context.allowSharedClusterTargets(); // multiple rules may target the same route
        return new Initialized(List.copyOf(compiledRules), ruleMatch.defaultRoute());
    }

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "init is marked @Nullable to match the @UnknownNullness contract of RouterFactory#createRouter,"
            + " but the runtime only invokes this method with the value returned by initialize, which is never null.")
    @Override
    public Router createRouter(RouterFactoryContext context, @Nullable Initialized init) {
        Objects.requireNonNull(init);
        return new SubjectRoutingHandler(init.rules(), init.defaultRoute(), context.routeNames(),
                context.virtualClusterName(), context.routerName());
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends Principal> resolvePrincipalType(String typeName) {
        Class<?> type;
        try {
            type = Class.forName(typeName);
        }
        catch (ClassNotFoundException e) {
            throw new PluginConfigurationException("ruleMatch hasPrincipals type '" + typeName + "' not found", e);
        }
        if (!Principal.class.isAssignableFrom(type)) {
            throw new PluginConfigurationException("ruleMatch hasPrincipals type '" + typeName + "' does not implement " + Principal.class.getName());
        }
        Class<? extends Principal> principalType = (Class<? extends Principal>) type;
        if (!SingularPrincipals.isSingular(principalType)) {
            throw new PluginConfigurationException("ruleMatch hasPrincipals type " + principalType.getName() + " is not a singular principal type");
        }
        return principalType;
    }
}
