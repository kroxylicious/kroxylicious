/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Tracks successful operations for potential rollback during configuration changes.
 * This class maintains a record of all cluster operations (removals, modifications, additions)
 * so they can be reversed if the overall configuration change fails.
 */
public class ConfigurationChangeRollbackTracker {
    private final List<String> removedClusters = new ArrayList<>();
    private final List<String> modifiedClusters = new ArrayList<>();
    private final List<String> addedClusters = new ArrayList<>();

    private final Map<String, VirtualClusterModel> removedModels = new HashMap<>();
    private final Map<String, VirtualClusterModel> originalModels = new HashMap<>();
    private final Map<String, VirtualClusterModel> modifiedModels = new HashMap<>();
    private final Map<String, VirtualClusterModel> addedModels = new HashMap<>();

    /**
     * Tracks a cluster removal operation.
     */
    public void trackRemoval(String clusterName, VirtualClusterModel removedModel) {
        removedClusters.add(clusterName);
        removedModels.put(clusterName, removedModel);
    }

    /**
     * Tracks a cluster modification operation.
     */
    public void trackModification(String clusterName, VirtualClusterModel originalModel, VirtualClusterModel newModel) {
        modifiedClusters.add(clusterName);
        originalModels.put(clusterName, originalModel);
        modifiedModels.put(clusterName, newModel);
    }

    /**
     * Tracks a cluster addition operation.
     */
    public void trackAddition(String clusterName, VirtualClusterModel addedModel) {
        addedClusters.add(clusterName);
        addedModels.put(clusterName, addedModel);
    }

    // Getter methods for rollback operations
    public List<String> getRemovedClusters() {
        return removedClusters;
    }

    public List<String> getModifiedClusters() {
        return modifiedClusters;
    }

    public List<String> getAddedClusters() {
        return addedClusters;
    }

    public VirtualClusterModel getRemovedModel(String clusterName) {
        return removedModels.get(clusterName);
    }

    public VirtualClusterModel getOriginalModel(String clusterName) {
        return originalModels.get(clusterName);
    }

    public VirtualClusterModel getModifiedModel(String clusterName) {
        return modifiedModels.get(clusterName);
    }

    public VirtualClusterModel getAddedModel(String clusterName) {
        return addedModels.get(clusterName);
    }

    /**
     * Gets the total number of operations tracked.
     */
    public int getTotalOperations() {
        return removedClusters.size() + modifiedClusters.size() + addedClusters.size();
    }
}