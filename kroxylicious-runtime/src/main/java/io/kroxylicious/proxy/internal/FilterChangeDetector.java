/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.VirtualCluster;
import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Change detector that identifies clusters needing restart due to filter configuration changes.
 * This includes:
 * - Changes to filter definitions (type or config modified)
 * - Changes to default filters (list or order changed)
 * - Changes to cluster-specific filter configurations
 * <p>
 * When a filter is modified, all virtual clusters using that filter (either directly or via defaults)
 * must be restarted to pick up the new filter configuration.
 */
public class FilterChangeDetector implements ChangeDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilterChangeDetector.class);

    @Override
    public String getName() {
        return "FilterChangeDetector";
    }

    @Override
    public ChangeResult detectChanges(ConfigurationChangeContext context) {
        // Detect filter definition changes
        Set<String> modifiedFilterNames = findModifiedFilterDefinitions(context);

        // Detect default filters changes (order matters for filter chain execution)
        boolean defaultFiltersChanged = hasDefaultFiltersChanged(context);

        // Find impacted clusters
        List<String> clustersToModify = findImpactedClusters(modifiedFilterNames, defaultFiltersChanged, context);

        // Build result
        ChangeResult result = new ChangeResult(List.of(), List.of(), clustersToModify);

        // Log results for observability
        if (!clustersToModify.isEmpty()) {
            LOGGER.info("{} detected {} clusters needing restart due to filter changes: modified filters={}, defaultFiltersChanged={}",
                    getName(), clustersToModify.size(), modifiedFilterNames, defaultFiltersChanged);
        }
        else {
            LOGGER.debug("{} detected no filter changes", getName());
        }

        LOGGER.debug("{} detected changes: {}", getName(), result.getSummary());

        return result;
    }

    /**
     * Find filter definitions that have been modified between old and new configuration.
     * A filter is considered modified if:
     * - The filter type changed
     * - The filter config changed
     * <p>
     * Note: Filter additions/removals are not tracked here as they're handled by Configuration validation.
     *
     * @param context the configuration change context
     * @return set of modified filter names
     */
    private Set<String> findModifiedFilterDefinitions(ConfigurationChangeContext context) {
        Map<String, NamedFilterDefinition> oldDefs = buildFilterDefMap(context.oldConfig());
        Map<String, NamedFilterDefinition> newDefs = buildFilterDefMap(context.newConfig());

        Set<String> modifiedFilterNames = new HashSet<>();

        // Check each new definition to see if it differs from the old one
        for (Map.Entry<String, NamedFilterDefinition> entry : newDefs.entrySet()) {
            String filterName = entry.getKey();
            NamedFilterDefinition newDef = entry.getValue();
            NamedFilterDefinition oldDef = oldDefs.get(filterName);

            // Filter exists in both configs - check if it changed
            if (oldDef != null && !oldDef.equals(newDef)) {
                modifiedFilterNames.add(filterName);
                LOGGER.debug("Filter '{}' has been modified (old={}, new={})", filterName, oldDef, newDef);
            }
        }

        return modifiedFilterNames;
    }

    /**
     * Build a map of filter definitions indexed by name.
     *
     * @param config the configuration
     * @return map of filter name to definition
     */
    private Map<String, NamedFilterDefinition> buildFilterDefMap(Configuration config) {
        List<NamedFilterDefinition> filterDefs = config.filterDefinitions();
        if (filterDefs == null || filterDefs.isEmpty()) {
            return Map.of();
        }

        Map<String, NamedFilterDefinition> map = new HashMap<>();
        for (NamedFilterDefinition def : filterDefs) {
            map.put(def.name(), def);
        }
        return map;
    }

    /**
     * Check if the default filters list has changed between old and new configuration.
     * Order matters because filter chain execution is sequential.
     *
     * @param context the configuration change context
     * @return true if default filters changed
     */
    private boolean hasDefaultFiltersChanged(ConfigurationChangeContext context) {
        List<String> oldDefaults = context.oldConfig().defaultFilters();
        List<String> newDefaults = context.newConfig().defaultFilters();

        // Use Objects.equals for null-safe comparison
        // This checks both list content AND order
        return !Objects.equals(oldDefaults, newDefaults);
    }

    /**
     * Find virtual clusters that are impacted by filter changes.
     * <p>
     * A cluster is impacted if:
     * - It uses a filter definition that was modified (either from explicit filters or defaults), OR
     * - It doesn't specify cluster.filters() AND defaultFilters list changed
     * <p>
     * Uses a simple single-pass approach: iterate through each cluster and check if it's
     * affected by any filter change. Prioritizes code clarity over optimization.
     *
     * @param modifiedFilterNames set of modified filter names
     * @param defaultFiltersChanged whether default filters changed
     * @param context the configuration change context
     * @return list of affected cluster names
     */
    private List<String> findImpactedClusters(
                                              Set<String> modifiedFilterNames,
                                              boolean defaultFiltersChanged,
                                              ConfigurationChangeContext context) {

        // Early return if nothing changed
        if (modifiedFilterNames.isEmpty() && !defaultFiltersChanged) {
            return List.of();
        }

        List<String> impactedClusters = new ArrayList<>();

        // Simple approach: check each cluster's resolved filters
        for (VirtualClusterModel cluster : context.newModels()) {
            String clusterName = cluster.getClusterName();

            // Get this cluster's resolved filters (either explicit or from defaults)
            List<String> clusterFilterNames = cluster.getFilters()
                    .stream()
                    .map(NamedFilterDefinition::name)
                    .toList();

            // Check if cluster uses any modified filter OR uses defaults and defaults changed
            boolean usesModifiedFilter = clusterFilterNames.stream()
                    .anyMatch(modifiedFilterNames::contains);

            boolean usesChangedDefaults = defaultFiltersChanged &&
                    clusterUsesDefaults(cluster, context.newConfig());

            if (usesModifiedFilter || usesChangedDefaults) {
                impactedClusters.add(clusterName);
                LOGGER.debug("Cluster '{}' impacted by filter changes, marking for restart", clusterName);
            }
        }

        return impactedClusters;
    }

    /**
     * Check if a cluster uses default filters.
     * A cluster uses defaults if it doesn't specify its own filters list.
     *
     * @param cluster the virtual cluster model
     * @param config the configuration
     * @return true if cluster uses default filters
     */
    private boolean clusterUsesDefaults(VirtualClusterModel cluster, Configuration config) {
        // Find the VirtualCluster config for this model
        VirtualCluster vc = config.virtualClusters().stream()
                .filter(v -> v.name().equals(cluster.getClusterName()))
                .findFirst()
                .orElse(null);

        // Cluster uses defaults if it doesn't specify its own filters
        return vc != null && vc.filters() == null;
    }
}
