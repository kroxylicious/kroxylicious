/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Handles configuration changes for the Kroxylicious proxy.
 * This class encapsulates all the logic for detecting, processing, and applying
 * configuration changes to virtual clusters.
 */
public class ConfigurationChangeHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigurationChangeHandler.class);

    private final List<ChangeDetector> changeDetectors;
    private final VirtualClusterManager virtualClusterManager;

    public ConfigurationChangeHandler(List<ChangeDetector> changeDetectors,
                                      VirtualClusterManager virtualClusterManager) {
        this.changeDetectors = List.copyOf(changeDetectors);
        this.virtualClusterManager = virtualClusterManager;

        LOGGER.info("Registered {} change detectors: {}",
                changeDetectors.size(),
                changeDetectors.stream().map(ChangeDetector::getName).collect(Collectors.toList()));
    }

    /**
     * Handles configuration changes detected by the file watcher.
     *
     * @param changeContext the configuration change context containing old and new configurations
     * @return CompletableFuture that completes when the configuration change is processed
     */
    public CompletableFuture<Void> handleConfigurationChange(
                                                             ConfigurationChangeContext changeContext) {
        try {

            LOGGER.info("Configuration change detected. Old configuration: {} clusters, New configuration: {} clusters",
                    changeContext.oldModels().size(), changeContext.newModels().size());

            // Use the change detectors to find what operations are needed
            ChangeResult changes = detectChanges(changeContext);

            if (!changes.hasChanges()) {
                LOGGER.info("No changes detected - hot-reload not needed");
                return CompletableFuture.completedFuture(null);
            }

            // Process changes in the correct order
            ConfigurationChangeRollbackTracker rollbackTracker = new ConfigurationChangeRollbackTracker();

            return processConfigurationChanges(changes, changeContext, rollbackTracker)
                    .thenRun(() -> {
                        LOGGER.info("Configuration hot-reload completed successfully - {} operations processed",
                                changes.getTotalOperations());
                    })
                    .whenComplete((result, throwable) -> {
                        if (throwable != null) {
                            LOGGER.error("Configuration change failed - initiating rollback", throwable);
                            performRollback(rollbackTracker)
                                    .whenComplete((rollbackResult, rollbackError) -> {
                                        if (rollbackError != null) {
                                            LOGGER.error("CRITICAL: Rollback failed - system may be in inconsistent state", rollbackError);
                                        }
                                        else {
                                            LOGGER.info("Rollback completed successfully - system restored to previous state");
                                        }
                                    });
                        }
                    });

        }
        catch (Exception e) {
            LOGGER.error("Failed to handle configuration change", e);
            return CompletableFuture.failedFuture(e);
        }
    }

    /**
     * Detects configuration changes by coordinating multiple change detectors.
     * This method aggregates results from all registered ChangeDetector implementations.
     *
     * @param context the configuration change context
     * @return aggregated change result from all detectors
     */
    private ChangeResult detectChanges(ConfigurationChangeContext context) {
        LOGGER.info("Analyzing configuration changes: {} old models -> {} new models",
                context.oldModels().size(), context.newModels().size());

        // Use LinkedHashSet to maintain order while removing duplicates
        Set<String> allClustersToRemove = new LinkedHashSet<>();
        Set<String> allClustersToAdd = new LinkedHashSet<>();
        Set<String> allClustersToModify = new LinkedHashSet<>();

        changeDetectors.forEach(detector -> {
            try {
                ChangeResult detectorResult = detector.detectChanges(context);
                LOGGER.debug("Detector '{}' found changes: {}",
                        detector.getName(), detectorResult.getSummary());

                allClustersToRemove.addAll(detectorResult.clustersToRemove());
                allClustersToAdd.addAll(detectorResult.clustersToAdd());
                allClustersToModify.addAll(detectorResult.clustersToModify());
            }
            catch (Exception e) {
                LOGGER.error("Error in change detector '{}': {}", detector.getName(), e.getMessage(), e);
                // Continue with other detectors even if one fails
            }
        });

        ChangeResult result = new ChangeResult(
                new ArrayList<>(allClustersToRemove),
                new ArrayList<>(allClustersToAdd),
                new ArrayList<>(allClustersToModify));

        LOGGER.info("Total changes detected: {} - Clusters to remove: {}, Clusters to add: {}, Clusters to modify: {}",
                result.getSummary(),
                result.clustersToRemove(),
                result.clustersToAdd(),
                result.clustersToModify());

        return result;
    }

    /**
     * Process configuration changes using the VirtualClusterManager with rollback support.
     * This method handles the orchestration of cluster operations in the correct order:
     * 1. Remove clusters first to free up resources
     * 2. Modify existing clusters
     * 3. Add new clusters last
     *
     * If any operation fails, all successful operations are rolled back.
     */
    private CompletableFuture<Void> processConfigurationChanges(
                                                                ChangeResult changes,
                                                                ConfigurationChangeContext changeContext,
                                                                ConfigurationChangeRollbackTracker rollbackTracker) {

        // Derive old models from the old configuration
        List<VirtualClusterModel> oldModels = changeContext.oldModels();

        // Create map for easy lookup of new models
        Map<String, VirtualClusterModel> newModelMap = changeContext.newModels().stream()
                .collect(Collectors.toMap(VirtualClusterModel::getClusterName, model -> model));

        CompletableFuture<Void> chain = CompletableFuture.completedFuture(null);

        // Step 1: Remove clusters first to free up ports/resources
        chain = changes.clustersToRemove().stream()
                .reduce(chain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    LOGGER.info("Removing cluster '{}'", clusterName);
                    return virtualClusterManager.removeVirtualCluster(clusterName, oldModels, rollbackTracker);
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        // Step 2: Modify existing clusters
        chain = changes.clustersToModify().stream()
                .reduce(chain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    VirtualClusterModel newModel = newModelMap.get(clusterName);
                    LOGGER.info("Modifying cluster '{}'", clusterName);
                    return virtualClusterManager.restartVirtualCluster(clusterName, newModel, oldModels, rollbackTracker);
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        // Step 3: Add new clusters last
        chain = changes.clustersToAdd().stream()
                .reduce(chain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    VirtualClusterModel newModel = newModelMap.get(clusterName);
                    LOGGER.info("Adding cluster '{}'", clusterName);
                    return virtualClusterManager.addVirtualCluster(newModel, rollbackTracker);
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        return chain;
    }

    /**
     * Performs rollback of successful operations when a failure occurs.
     */
    private CompletableFuture<Void> performRollback(ConfigurationChangeRollbackTracker tracker) {
        LOGGER.warn("Starting rollback of {} operations", tracker.getTotalOperations());

        CompletableFuture<Void> rollbackChain = CompletableFuture.completedFuture(null);

        // Created a dummy tracker to avoid tracking during rollback
        ConfigurationChangeRollbackTracker dummyTracker = new ConfigurationChangeRollbackTracker();

        // Rollback in reverse order
        // 1. Remove clusters that were added
        rollbackChain = tracker.getAddedClusters().stream()
                .reduce(rollbackChain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    LOGGER.info("Rolling back addition of cluster '{}'", clusterName);
                    return virtualClusterManager.removeVirtualCluster(clusterName, List.of(tracker.getAddedModel(clusterName)), dummyTracker)
                            .exceptionally(rollbackError -> {
                                LOGGER.error("Failed to rollback addition of cluster '{}': {}", clusterName, rollbackError.getMessage());
                                return null; // Continue with other rollbacks
                            });
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        // 2. Restore clusters that were modified
        rollbackChain = tracker.getModifiedClusters().stream()
                .reduce(rollbackChain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    VirtualClusterModel originalModel = tracker.getOriginalModel(clusterName);
                    LOGGER.info("Rolling back modification of cluster '{}' to original state", clusterName);
                    return virtualClusterManager.restartVirtualCluster(clusterName, originalModel, List.of(tracker.getModifiedModel(clusterName)), dummyTracker)
                            .exceptionally(rollbackError -> {
                                LOGGER.error("Failed to rollback modification of cluster '{}': {}", clusterName, rollbackError.getMessage());
                                return null; // Continue with other rollbacks
                            });
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        // 3. Re-add clusters that were removed
        rollbackChain = tracker.getRemovedClusters().stream()
                .reduce(rollbackChain, (currentChain, clusterName) -> currentChain.thenCompose(v -> {
                    VirtualClusterModel originalModel = tracker.getRemovedModel(clusterName);
                    LOGGER.info("Rolling back removal of cluster '{}' by re-adding it", clusterName);
                    return virtualClusterManager.addVirtualCluster(originalModel, dummyTracker) // Automatically stops draining
                            .exceptionally(rollbackError -> {
                                LOGGER.error("Failed to rollback removal of cluster '{}': {}", clusterName, rollbackError.getMessage());
                                return null; // Continue with other rollbacks
                            });
                }),
                        (chain1, chain2) -> chain1.thenCompose(v -> chain2));

        return rollbackChain;
    }

}
