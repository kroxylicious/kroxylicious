/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.router.client;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.plugin.PluginConfigurationException;
import io.kroxylicious.proxy.plugin.Plugins;
import io.kroxylicious.proxy.router.Router;
import io.kroxylicious.proxy.router.RouterFactory;
import io.kroxylicious.proxy.router.RouterFactoryContext;

import edu.umd.cs.findbugs.annotations.Nullable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * A {@link RouterFactory} that routes each client connection to a downstream cluster based on attributes
 * of the client like clientId, matched against an ordered list of rules.
 */
@Plugin(configType = Config.class)
public class ClientRouter implements RouterFactory<Config, ClientRouter.Initialized> {

    /**
     * Creates a {@link ClientRouter}.
     */
    public ClientRouter() {
    }

    /**
     * Key used to detect two rules with identical predicates at startup.
     */
    private record PredicateKey(@Nullable String hasClientId) {}

    /**
     * Immutable, thread-safe: shared across all connections of a virtual cluster.
     *
     * @param rules ordered, compiled rules
     */
    public record Initialized(List<Rule> rules) {}

    @Override
    public Initialized initialize(RouterFactoryContext context, Config config) throws PluginConfigurationException {
        Config cfg = Plugins.requireConfig(this, config);
        List<Rule> routingRules = cfg.routingRules();

        Set<PredicateKey> seenPredicates = new HashSet<>();
        List<Rule> compiledRules = new ArrayList<>();
        for (Rule rule : routingRules) {
            if (rule.hasClientId() == null) {
                throw new PluginConfigurationException("routingRules entry needs hasClientId");
            }
            if (rule.useRoute() == null) {
                throw new PluginConfigurationException("routingRules entry needs useRoute");
            }
            if (!seenPredicates.add(new PredicateKey(rule.hasClientId()))) {
                throw new PluginConfigurationException(
                        "routingRules contains more than one rule with hasClientId " + rule.hasClientId());
            }
            compiledRules.add(new Rule(rule.hasClientId(), rule.useRoute()));
        }

        Set<String> routeNames = context.routeNames();
        Set<String> referenced = new HashSet<>();
        compiledRules.forEach(rule -> referenced.add(rule.useRoute()));
        referenced.removeAll(routeNames);
        if (!referenced.isEmpty()) {
            throw new PluginConfigurationException(
                    "routingRules references unknown routes " + referenced + "; declared routes are " + routeNames);
        }

        context.allowSharedClusterTargets(); // multiple rules may target the same route
        return new Initialized(List.copyOf(compiledRules));
    }

    @SuppressFBWarnings(value = "NP_PARAMETER_MUST_BE_NONNULL_BUT_MARKED_AS_NULLABLE", justification = "init is marked @Nullable to match the @UnknownNullness contract of RouterFactory#createRouter,"
            + " but the runtime only invokes this method with the value returned by initialize, which is never null.")
    @Override
    public Router createRouter(RouterFactoryContext context, @Nullable Initialized init) {
        Objects.requireNonNull(init);
        return new ClientRoutingRouter(init.rules(), context.virtualClusterName(), context.routerName());
    }

}
