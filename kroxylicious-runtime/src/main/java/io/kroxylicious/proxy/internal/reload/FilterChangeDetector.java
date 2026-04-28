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

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;

/**
 * Identifies virtual clusters that need to be restarted because a filter configuration they
 * depend on has changed.
 * <p>
 * A cluster is affected if either:
 * <ul>
 *   <li>any {@link NamedFilterDefinition} it references (directly or via {@code defaultFilters})
 *       changed — i.e. the named entry's type or config is different between old and new; or</li>
 *   <li>the cluster relies on {@code defaultFilters} and that list changed (order matters because
 *       the filter chain executes sequentially).</li>
 * </ul>
 * This detector only produces {@code clustersToModify} entries. Added and removed clusters are
 * the concern of {@link VirtualClusterChangeDetector}.
 */
public class FilterChangeDetector implements ChangeDetector {

    @Override
    public ChangeResult detect(ConfigurationChangeContext context) {
        Configuration oldConfig = context.oldConfig();
        Configuration newConfig = context.newConfig();

        Set<String> changedFilterNames = changedFilterNames(oldConfig, newConfig);
        boolean defaultFiltersChanged = defaultFiltersChanged(oldConfig, newConfig);

        // Early-out: if neither filter definitions nor default-filter ordering changed, no cluster
        // can be affected by a filter-level change, so we can skip the per-cluster scan.
        if (changedFilterNames.isEmpty() && !defaultFiltersChanged) {
            return ChangeResult.EMPTY;
        }

        Map<String, VirtualCluster> newByName = newConfig.virtualClusters().stream()
                .collect(Collectors.toMap(VirtualCluster::name, Function.identity()));

        // We iterate OLD clusters (not new) and look up by name in the new map because:
        // - Pure additions are VirtualClusterChangeDetector's concern, not ours.
        // - We only care about clusters that existed before AND still exist — i.e. candidates
        // for "modify" — so the old-config list is the right starting set.
        Set<String> toModify = new HashSet<>();
        for (VirtualCluster oldCluster : oldConfig.virtualClusters()) {
            VirtualCluster newCluster = newByName.get(oldCluster.name());
            if (newCluster == null) {
                // Removed — VirtualClusterChangeDetector will flag this as clustersToRemove.
                continue;
            }
            if (referencesChangedFilter(newCluster, newConfig, changedFilterNames, defaultFiltersChanged)) {
                toModify.add(newCluster.name());
            }
        }

        return new ChangeResult(Set.of(), Set.of(), toModify);
    }

    /**
     * Names of filter definitions whose type or configuration changed between old and new.
     * <p>
     * Includes additions (missing in old, present in new) and removals (present in old, missing
     * in new) as well as true modifications. For change-impact purposes, all three are equivalent
     * — any cluster referencing such a name needs to be restarted to pick up the new chain.
     */
    private static Set<String> changedFilterNames(Configuration oldConfig, Configuration newConfig) {
        return KeyedListEquality.changedKeys(
                oldConfig.filterDefinitions(),
                newConfig.filterDefinitions(),
                NamedFilterDefinition::name);
    }

    private static boolean defaultFiltersChanged(Configuration oldConfig, Configuration newConfig) {
        // Order-sensitive comparison: filter chain execution is sequential, so reordering
        // the same names is still a semantic change — the filters will run in a different
        // order and may produce different results. List.equals() already honours order, so
        // Objects.equals() on the lists is the right check.
        return !Objects.equals(oldConfig.defaultFilters(), newConfig.defaultFilters());
    }

    /**
     * Decides whether the given cluster's filter chain is affected by the change.
     * <p>
     * A {@code null} cluster.filters() means "use the top-level defaultFilters" — both that
     * nullability and the defaultFilters-changed shortcut exist because operators often manage
     * a fleet of clusters that share one chain, so the "use defaults" case needs its own path.
     */
    private static boolean referencesChangedFilter(VirtualCluster cluster,
                                                   Configuration newConfig,
                                                   Set<String> changedFilterNames,
                                                   boolean defaultFiltersChanged) {
        List<String> filters = cluster.filters();
        if (filters == null) {
            // Cluster relies on defaultFilters. Any reorder, addition, or removal there — even
            // without any individual filter definition changing — means this cluster's chain
            // is different and it must be restarted.
            if (defaultFiltersChanged) {
                return true;
            }
            // defaultFilters list itself unchanged, but the cluster could still be affected by
            // a modified definition of one of the filters it inherits from defaults.
            filters = newConfig.defaultFilters() == null ? List.of() : newConfig.defaultFilters();
        }
        for (String filterName : filters) {
            if (changedFilterNames.contains(filterName)) {
                return true;
            }
        }
        return false;
    }
}
