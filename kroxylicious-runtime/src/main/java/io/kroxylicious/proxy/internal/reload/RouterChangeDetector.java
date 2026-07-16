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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.kroxylicious.proxy.config.ClusterDefinition;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.RouterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Identifies virtual clusters that need to be restarted because a router definition they
 * depend on has changed.
 * <p>
 * A cluster is affected if any router in its router graph (starting from the cluster's
 * {@code target.router} and following nested route targets recursively) changed between
 * old and new configuration — i.e. the router's type, config, or routes differ.
 * <p>
 * This detector only produces {@code clustersToModify} entries. Added and removed clusters
 * are the concern of {@link VirtualClusterChangeDetector}.
 */
final class RouterChangeDetector implements ChangeDetector {

    @Override
    public ChangeResult detect(ConfigurationChangeContext context) {
        Configuration oldConfig = context.oldConfig();
        Configuration newConfig = context.newConfig();

        Set<String> changedRouterNames = changedRouterNames(oldConfig, newConfig);
        if (changedRouterNames.isEmpty()) {
            return ChangeResult.EMPTY;
        }

        Map<String, VirtualCluster> newByName = newConfig.virtualClusters().stream()
                .collect(Collectors.toMap(VirtualCluster::name, Function.identity()));
        Map<String, RouterDefinition> newRoutersByName = indexRouterDefinitionsByName(newConfig.routerDefinitions());
        Map<String, ClusterDefinition> newClustersByName = newConfig.clusterDefinitions() == null ? Map.of()
                : newConfig.clusterDefinitions().stream().collect(Collectors.toMap(ClusterDefinition::name, Function.identity()));

        Set<String> toModify = new HashSet<>();
        for (VirtualCluster oldCluster : oldConfig.virtualClusters()) {
            VirtualCluster newCluster = newByName.get(oldCluster.name());
            if (newCluster == null) {
                // Removed — VirtualClusterChangeDetector will flag this as clustersToRemove.
                continue;
            }
            if (transitivelyReferencesChangedRouter(newCluster, newRoutersByName, newClustersByName, changedRouterNames)) {
                toModify.add(newCluster.name());
            }
        }

        return new ChangeResult(Set.of(), Set.of(), toModify);
    }

    /**
     * Names of router definitions whose content changed between old and new config.
     * Additions and removals are treated as changes because any VC referencing that
     * name needs to be restarted to pick up the updated router state.
     */
    private static Set<String> changedRouterNames(Configuration oldConfig, Configuration newConfig) {
        Map<String, RouterDefinition> oldByName = indexRouterDefinitionsByName(oldConfig.routerDefinitions());
        Map<String, RouterDefinition> newByName = indexRouterDefinitionsByName(newConfig.routerDefinitions());

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

    /**
     * Returns {@code true} if the VC's router graph (traversed recursively from its root router)
     * contains any router whose name is in {@code changedRouterNames}.
     */
    private static boolean transitivelyReferencesChangedRouter(VirtualCluster vc,
                                                               Map<String, RouterDefinition> routersByName,
                                                               Map<String, ClusterDefinition> clustersByName,
                                                               Set<String> changedRouterNames) {
        return ClusterGraphWalker.anyInClusterGraph(vc, routersByName, clustersByName, changedRouterNames::contains, name -> false);
    }
}
