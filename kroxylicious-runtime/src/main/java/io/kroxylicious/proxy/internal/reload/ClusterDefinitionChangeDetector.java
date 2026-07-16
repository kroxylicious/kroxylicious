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
 * Identifies virtual clusters that need to be restarted because a cluster definition they
 * reference has changed.
 * <p>
 * A cluster is affected if it names a cluster definition (via {@code target.cluster()}) whose
 * content — bootstrap servers or TLS config — differs between the old and new configuration,
 * or if it targets a router whose DAG transitively references a changed cluster definition.
 * <p>
 * VCs using an inline {@code targetCluster} are not affected by this detector.
 * <p>
 * This detector only produces {@code clustersToModify} entries. Added and removed clusters
 * are the concern of {@link VirtualClusterChangeDetector}.
 */
final class ClusterDefinitionChangeDetector implements ChangeDetector {

    @Override
    public ChangeResult detect(ConfigurationChangeContext context) {
        Configuration oldConfig = context.oldConfig();
        Configuration newConfig = context.newConfig();

        Set<String> changedClusterNames = changedClusterNames(oldConfig, newConfig);
        if (changedClusterNames.isEmpty()) {
            return ChangeResult.EMPTY;
        }

        Map<String, VirtualCluster> newByName = newConfig.virtualClusters().stream()
                .collect(Collectors.toMap(VirtualCluster::name, Function.identity()));
        Map<String, RouterDefinition> newRoutersByName = indexRouterDefinitionsByName(newConfig.routerDefinitions());
        Map<String, ClusterDefinition> newClustersByName = indexByName(newConfig.clusterDefinitions());

        Set<String> toModify = new HashSet<>();
        for (VirtualCluster oldCluster : oldConfig.virtualClusters()) {
            VirtualCluster newCluster = newByName.get(oldCluster.name());
            if (newCluster == null) {
                // Removed — VirtualClusterChangeDetector will flag this as clustersToRemove.
                continue;
            }
            if (ClusterGraphWalker.anyInClusterGraph(newCluster, newRoutersByName, newClustersByName, name -> false, changedClusterNames::contains)) {
                toModify.add(newCluster.name());
            }
        }

        return new ChangeResult(Set.of(), Set.of(), toModify);
    }

    private static Map<String, RouterDefinition> indexRouterDefinitionsByName(@Nullable List<RouterDefinition> defs) {
        return defs == null ? Map.of()
                : defs.stream().collect(Collectors.toMap(RouterDefinition::name, Function.identity()));
    }

    /**
     * Names of cluster definitions whose content changed between old and new config.
     * Additions and removals are treated as changes because any VC referencing that
     * name needs to be restarted to pick up the updated connection parameters.
     */
    private static Set<String> changedClusterNames(Configuration oldConfig, Configuration newConfig) {
        Map<String, ClusterDefinition> oldByName = indexByName(oldConfig.clusterDefinitions());
        Map<String, ClusterDefinition> newByName = indexByName(newConfig.clusterDefinitions());

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

    private static Map<String, ClusterDefinition> indexByName(@Nullable List<ClusterDefinition> defs) {
        return defs == null ? Map.of()
                : defs.stream().collect(Collectors.toMap(ClusterDefinition::name, Function.identity()));
    }
}
