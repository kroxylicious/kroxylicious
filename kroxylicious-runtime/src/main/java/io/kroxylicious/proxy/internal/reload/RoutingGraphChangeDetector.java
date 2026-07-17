/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.RoutingGraphVisitor;
import io.kroxylicious.proxy.config.RoutingGraphWalker;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.config.WalkContext;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Identifies virtual clusters that need to be restarted because a router definition or
 * cluster definition they depend on has changed.
 * <p>
 * Pre-computes the sets of changed router and cluster definition names, then performs a
 * single walk of each VC's routing graph testing both predicates at once. This avoids the
 * redundant per-concern walks that would result from separate router and cluster-definition
 * detectors each independently traversing the same graph.
 * <p>
 * This detector only produces {@code clustersToModify} entries. Added and removed clusters
 * are the concern of {@link VirtualClusterChangeDetector}.
 */
final class RoutingGraphChangeDetector implements ChangeDetector {

    @Override
    public ChangeResult detect(ConfigurationChangeContext context) {
        Configuration oldConfig = context.oldConfig();
        Configuration newConfig = context.newConfig();

        Set<String> changedRouterNames = changedRouterNames(oldConfig, newConfig);
        Set<String> changedClusterNames = changedClusterNames(oldConfig, newConfig);

        if (changedRouterNames.isEmpty() && changedClusterNames.isEmpty()) {
            return ChangeResult.EMPTY;
        }

        Map<String, VirtualCluster> newByName = newConfig.virtualClusters().stream()
                .collect(Collectors.toMap(VirtualCluster::name, Function.identity()));
        Map<String, RouterDefinition> newRoutersByName = indexRouterDefinitionsByName(newConfig.routerDefinitions());
        Map<String, ClusterDefinition> newClustersByName = indexClusterDefinitionsByName(newConfig.clusterDefinitions());

        Set<String> toModify = new HashSet<>();
        for (VirtualCluster oldCluster : oldConfig.virtualClusters()) {
            VirtualCluster newCluster = newByName.get(oldCluster.name());
            if (newCluster == null) {
                continue;
            }
            if (anyInClusterGraph(newCluster, newRoutersByName, newClustersByName,
                    changedRouterNames::contains, changedClusterNames::contains)) {
                toModify.add(newCluster.name());
            }
        }

        return new ChangeResult(Set.of(), Set.of(), toModify);
    }

    /**
     * Returns {@code true} if any node reachable from {@code vc} in its routing graph
     * satisfies either predicate.
     *
     * @param vc                   the virtual cluster whose graph is walked
     * @param routersByName        all router definitions indexed by name
     * @param clustersByName       all cluster definitions indexed by name
     * @param routerNamePredicate  test applied to each visited router definition's name
     * @param clusterNamePredicate test applied to each cluster definition's name at a leaf
     */
    @VisibleForTesting
    static boolean anyInClusterGraph(VirtualCluster vc,
                                            Map<String, RouterDefinition> routersByName,
                                            Map<String, ClusterDefinition> clustersByName,
                                            Predicate<String> routerNamePredicate,
                                            Predicate<String> clusterNamePredicate) {
        return Objects.requireNonNull(RoutingGraphWalker.walkClusterGraph(vc, routersByName, clustersByName,
                () -> new AnyMatchVisitor(routerNamePredicate, clusterNamePredicate)));
    }

    private static final class AnyMatchVisitor implements RoutingGraphVisitor<Boolean> {

        private final Predicate<String> routerNamePredicate;
        private final Predicate<String> clusterNamePredicate;
        private boolean found;

        AnyMatchVisitor(Predicate<String> routerNamePredicate, Predicate<String> clusterNamePredicate) {
            this.routerNamePredicate = routerNamePredicate;
            this.clusterNamePredicate = clusterNamePredicate;
        }

        @Override
        public boolean enterRouter(RouterDefinition rd, WalkContext ctx) {
            if (ctx.isFirstVisit() && routerNamePredicate.test(rd.name())) {
                found = true;
                return false;
            }
            return true;
        }

        @Override
        public boolean visitClusterDefinition(ClusterDefinition cd, WalkContext ctx) {
            if (clusterNamePredicate.test(cd.name())) {
                found = true;
                return false;
            }
            return true;
        }

        @Override
        public Boolean result() {
            return found;
        }
    }

    private static Set<String> changedRouterNames(Configuration oldConfig, Configuration newConfig) {
        Map<String, RouterDefinition> oldByName = indexRouterDefinitionsByName(oldConfig.routerDefinitions());
        Map<String, RouterDefinition> newByName = indexRouterDefinitionsByName(newConfig.routerDefinitions());
        return changedNames(oldByName, newByName);
    }

    private static Set<String> changedClusterNames(Configuration oldConfig, Configuration newConfig) {
        Map<String, ClusterDefinition> oldByName = indexClusterDefinitionsByName(oldConfig.clusterDefinitions());
        Map<String, ClusterDefinition> newByName = indexClusterDefinitionsByName(newConfig.clusterDefinitions());
        return changedNames(oldByName, newByName);
    }

    private static <T> Set<String> changedNames(Map<String, T> oldByName, Map<String, T> newByName) {
        Set<String> changed = new HashSet<>();
        Stream.concat(oldByName.keySet().stream(), newByName.keySet().stream())
                .distinct()
                .forEach(name -> {
                    if (!Objects.equals(oldByName.get(name), newByName.get(name))) {
                        changed.add(name);
                    }
                });
        return changed;
    }

    private static Map<String, RouterDefinition> indexRouterDefinitionsByName(@Nullable List<RouterDefinition> defs) {
        return defs == null ? Map.of()
                : defs.stream().collect(Collectors.toMap(RouterDefinition::name, Function.identity()));
    }

    private static Map<String, ClusterDefinition> indexClusterDefinitionsByName(@Nullable List<ClusterDefinition> defs) {
        return defs == null ? Map.of()
                : defs.stream().collect(Collectors.toMap(ClusterDefinition::name, Function.identity()));
    }
}
