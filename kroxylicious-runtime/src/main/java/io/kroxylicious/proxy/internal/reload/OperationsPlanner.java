/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Turns a {@link ChangeResult} plus the submitted {@link Configuration} into an ordered
 * list of {@link ClusterOperation}s for the orchestrator to execute. Stateless — the
 * planner holds only its collaborators, no per-reconfigure state.
 *
 * <p>Ordering: removes before adds</p>
 */
final class OperationsPlanner {

    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;
    private final Function<Configuration, List<VirtualClusterModel>> modelResolver;

    OperationsPlanner(VirtualClusterRegistry virtualClusterRegistry,
                      EndpointRegistry endpointRegistry,
                      Function<Configuration, List<VirtualClusterModel>> modelResolver) {
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        this.modelResolver = Objects.requireNonNull(modelResolver, "modelResolver");
    }

    /**
     * Plan the operations for a reconfigure.
     *
     * @throws IllegalStateException if {@code changes.clustersToRemove()} names a cluster that
     *         isn't registered, or {@code changes.clustersToAdd()} names a cluster that isn't
     *         in {@code newConfig}'s resolved models — both indicate a {@code ChangeDetector}
     *         contract violation (i.e. a framework bug).
     */
    List<ClusterOperation> plan(ChangeResult changes, Configuration newConfig) {
        var ops = new ArrayList<ClusterOperation>();

        if (!changes.clustersToRemove().isEmpty()) {
            Map<String, VirtualClusterModel> registryModelsByName = registryModelsByName();
            for (String name : changes.clustersToRemove()) {
                VirtualClusterModel model = registryModelsByName.get(name);
                if (model == null) {
                    throw new IllegalStateException(
                            "OperationsPlanner: no model for removed cluster '" + name
                                    + "'; this indicates a ChangeDetector contract violation"
                                    + " (cluster reported as removed but absent from the registry)");
                }
                ops.add(new RemoveCluster(model, virtualClusterRegistry, endpointRegistry));
            }
        }

        if (!changes.clustersToAdd().isEmpty()) {
            Map<String, VirtualClusterModel> newModelsByName = resolveByName(newConfig);
            for (String name : changes.clustersToAdd()) {
                VirtualClusterModel model = newModelsByName.get(name);
                if (model == null) {
                    throw new IllegalStateException(
                            "OperationsPlanner: no model for added cluster '" + name
                                    + "'; this indicates a ChangeDetector contract violation"
                                    + " (cluster reported as added but absent from the submitted configuration)");
                }
                ops.add(new AddCluster(model, virtualClusterRegistry, endpointRegistry));
            }
        }

        return List.copyOf(ops);
    }

    private Map<String, VirtualClusterModel> resolveByName(Configuration newConfig) {
        return modelResolver.apply(newConfig).stream()
                .collect(Collectors.toUnmodifiableMap(VirtualClusterModel::getClusterName, Function.identity()));
    }

    private Map<String, VirtualClusterModel> registryModelsByName() {
        return virtualClusterRegistry.virtualClusterModels().stream()
                .collect(Collectors.toUnmodifiableMap(VirtualClusterModel::getClusterName, Function.identity()));
    }
}
