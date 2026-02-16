/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Manages virtual cluster lifecycle operations including selective restarts, additions, and removals.
 * This class handles the complex orchestration of cluster operations during hot-reload scenarios.
 */
public class VirtualClusterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterManager.class);

    private final EndpointRegistry endpointRegistry;
    private final ConnectionDrainManager connectionDrainManager;

    public VirtualClusterManager(EndpointRegistry endpointRegistry, ConnectionDrainManager connectionDrainManager) {
        this.endpointRegistry = endpointRegistry;
        this.connectionDrainManager = connectionDrainManager;
    }

    /**
     * Removes a virtual cluster by draining connections and deregistering from endpoint registry.
     */
    public CompletableFuture<Void> removeVirtualCluster(String clusterName, List<VirtualClusterModel> oldModels,
                                                        ConfigurationChangeRollbackTracker rollbackTracker) {
        // Find the cluster model to remove
        VirtualClusterModel clusterToRemove = oldModels.stream()
                .filter(model -> model.getClusterName().equals(clusterName))
                .findFirst()
                .orElse(null);

        if (clusterToRemove == null) {
            LOGGER.warn("Cannot remove virtual cluster '{}' - not found in old models", clusterName);
            return CompletableFuture.completedFuture(null);
        }

        // Drain connections first
        return connectionDrainManager.gracefullyDrainConnections(clusterName, Duration.ofSeconds(30))
                .thenCompose(v -> {
                    // Deregister all gateways for this cluster
                    var deregistrationFutures = clusterToRemove.gateways().values().stream()
                            .map(gateway -> {
                                LOGGER.debug("Deregistering gateway '{}' for removed cluster '{}'", gateway.name(), clusterName);
                                return endpointRegistry.deregisterVirtualCluster(gateway);
                            })
                            .toArray(CompletableFuture[]::new);

                    return CompletableFuture.allOf(deregistrationFutures);
                })
                .thenRun(() -> {
                    // Track the removal for potential rollback
                    rollbackTracker.trackRemoval(clusterName, clusterToRemove);
                    LOGGER.info("Successfully removed virtual cluster '{}'", clusterName);
                });
    }

    /**
     * Restarts a virtual cluster with a new configuration model.
     */
    public CompletableFuture<Void> restartVirtualCluster(String clusterName, VirtualClusterModel newModel,
                                                         List<VirtualClusterModel> oldModels,
                                                         ConfigurationChangeRollbackTracker rollbackTracker) {
        // Find the old model for tracking
        VirtualClusterModel oldModel = oldModels.stream()
                .filter(model -> model.getClusterName().equals(clusterName))
                .findFirst()
                .orElse(null);

        if (oldModel == null) {
            return CompletableFuture.failedFuture(
                    new IllegalArgumentException("Cannot restart cluster '" + clusterName + "' - not found in old models"));
        }

        // Step 1: Remove the existing cluster (drain connections + deregister gateways)
        return removeVirtualCluster(clusterName, oldModels, rollbackTracker)
                .thenCompose(v -> {
                    // Step 2: Add the new cluster with updated configuration
                    LOGGER.debug("Adding new configuration for restarted cluster '{}'", clusterName);
                    return addVirtualCluster(newModel, rollbackTracker);
                })
                .thenRun(() -> {
                    // Step 3: Track the modification and stop draining
                    rollbackTracker.trackModification(clusterName, oldModel, newModel);
                    connectionDrainManager.stopDraining(clusterName);
                    LOGGER.info("Successfully restarted virtual cluster '{}' with new configuration", clusterName);
                });
    }

    /**
     * Adds a new virtual cluster.
     */
    public CompletableFuture<Void> addVirtualCluster(VirtualClusterModel newModel,
                                                     ConfigurationChangeRollbackTracker rollbackTracker) {
        String clusterName = newModel.getClusterName();

        return registerVirtualCluster(newModel)
                .thenRun(() -> {
                    // Track the addition and stop draining to allow new connections
                    rollbackTracker.trackAddition(clusterName, newModel);
                    connectionDrainManager.stopDraining(clusterName);
                    LOGGER.info("Successfully added new virtual cluster '{}'", clusterName);
                });
    }

    /**
     * Registers a single virtual cluster with the endpoint registry.
     */
    private CompletableFuture<Void> registerVirtualCluster(VirtualClusterModel clusterModel) {
        String clusterName = clusterModel.getClusterName();
        LOGGER.info("Registering virtual cluster '{}' with {} gateways", clusterName, clusterModel.gateways().size());

        var registrationFutures = clusterModel.gateways().values().stream()
                .map(gateway -> {
                    LOGGER.debug("Registering gateway: {}", gateway.name());
                    return endpointRegistry.registerVirtualCluster(gateway);
                })
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(registrationFutures)
                .thenRun(() -> {
                    LOGGER.info("Successfully registered virtual cluster '{}' with all gateways", clusterName);
                })
                .exceptionally(throwable -> {
                    throw new RuntimeException("Failed to register virtual cluster '" + clusterName + "'", throwable);
                });
    }
}
