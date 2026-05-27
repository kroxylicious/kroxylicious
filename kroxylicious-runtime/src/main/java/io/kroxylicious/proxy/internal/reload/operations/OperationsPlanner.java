/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.internal.reload.ChangeResult;
import io.kroxylicious.proxy.model.VirtualClusterModel;

/**
 * Turns a {@link ChangeResult} plus the submitted {@link Configuration} into an ordered
 * list of {@link ClusterOperation}s for the orchestrator to execute. Stateless — the
 * planner holds only its collaborators, no per-reconfigure state.
 *
 * <p>Ordering: removes before adds</p>
 */
public final class OperationsPlanner {

    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;
    private final Function<Configuration, List<VirtualClusterModel>> modelResolver;

    public OperationsPlanner(VirtualClusterRegistry virtualClusterRegistry,
                             EndpointRegistry endpointRegistry,
                             Function<Configuration, List<VirtualClusterModel>> modelResolver) {
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        this.modelResolver = Objects.requireNonNull(modelResolver, "modelResolver");
    }

    /**
     * Plan the operations for a reconfigure.
     *
     * @throws IllegalStateException if {@code changes.clustersToAdd()} names a cluster that
     *         isn't present in {@code newConfig}'s resolved models — this indicates a
     *         {@code ChangeDetector} contract violation (i.e. a framework bug).
     */
    public List<ClusterOperation> plan(ChangeResult changes, Configuration newConfig) {
        var ops = new ArrayList<ClusterOperation>();

        for (String name : changes.clustersToRemove()) {
            ops.add(new RemoveCluster(name, virtualClusterRegistry, endpointRegistry));
        }

        if (!changes.clustersToAdd().isEmpty()) {
            Map<String, VirtualClusterModel> modelsByName = resolveByName(newConfig);
            for (String name : changes.clustersToAdd()) {
                VirtualClusterModel model = modelsByName.get(name);
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
                .collect(Collectors.toUnmodifiableMap(VirtualClusterModel::getClusterName, m -> m));
    }
}
