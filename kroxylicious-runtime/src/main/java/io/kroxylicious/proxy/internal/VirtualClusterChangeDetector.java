/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Change detector that identifies virtual clusters needing restart due to configuration changes.
 * This includes:
 * - New clusters (additions)
 * - Removed clusters (deletions)
 * - Modified clusters (configuration changes)
 */
public class VirtualClusterChangeDetector implements ChangeDetector {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterChangeDetector.class);

    @Override
    public String getName() {
        return "VirtualClusterChangeDetector";
    }

    @Override
    public ChangeResult detectChanges(ConfigurationChangeContext context) {
        // Check for modified clusters
        List<String> modifiedClusters = findModifiedClusters(context);
        LOGGER.debug("Found {} modified clusters: {}", modifiedClusters.size(), modifiedClusters);

        // Check for new clusters
        List<String> newClusters = findNewClusters(context);
        LOGGER.debug("Found {} new clusters: {}", newClusters.size(), newClusters);

        // Check for removed clusters
        List<String> removedClusters = findRemovedClusters(context);
        LOGGER.debug("Found {} removed clusters: {}", removedClusters.size(), removedClusters);

        return new ChangeResult(removedClusters, newClusters, modifiedClusters);
    }

    /**
     * Find clusters that exist in both old and new configurations but have different models.
     */
    private List<String> findModifiedClusters(ConfigurationChangeContext context) {
        Map<String, VirtualClusterModel> oldModelMap = context.oldModels().stream()
                .collect(Collectors.toMap(VirtualClusterModel::getClusterName, model -> model));

        return context.newModels().stream()
                .filter(newModel -> {
                    VirtualClusterModel oldModel = oldModelMap.get(newModel.getClusterName());
                    if (oldModel == null) {
                        return false; // This is a new cluster, not a modification
                    }
                    return !oldModel.equals(newModel);
                })
                .map(VirtualClusterModel::getClusterName)
                .collect(Collectors.toList());
    }

    /**
     * Find clusters that exist in new configuration but not in old configuration.
     */
    private List<String> findNewClusters(ConfigurationChangeContext context) {
        Set<String> oldClusterNames = context.oldModels().stream()
                .map(VirtualClusterModel::getClusterName)
                .collect(Collectors.toSet());

        return context.newModels().stream()
                .map(VirtualClusterModel::getClusterName)
                .filter(name -> !oldClusterNames.contains(name))
                .collect(Collectors.toList());
    }

    /**
     * Find clusters that exist in old configuration but not in new configuration.
     */
    private List<String> findRemovedClusters(ConfigurationChangeContext context) {
        Set<String> newClusterNames = context.newModels().stream()
                .map(VirtualClusterModel::getClusterName)
                .collect(Collectors.toSet());

        return context.oldModels().stream()
                .map(VirtualClusterModel::getClusterName)
                .filter(name -> !newClusterNames.contains(name))
                .collect(Collectors.toList());
    }
}
