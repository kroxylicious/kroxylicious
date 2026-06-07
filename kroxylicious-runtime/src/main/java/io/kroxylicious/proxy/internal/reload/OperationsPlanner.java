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
 * <p><strong>Ordering:</strong> pure removes → modifies → pure adds. The "removes before
 * adds" invariant is preserved globally, and each modify is a self-contained pair-wise
 * remove-then-add inside its {@link ReplaceCluster} operation — so a same-port modify gets
 * unbind-then-rebind sequenced tightly within one op, without intervening drains from
 * unrelated clusters.</p>
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
     * Plan the operations for a reconfigure. Each phase resolves a {@link VirtualClusterModel}
     * by name and hands it to the operation: removes resolve from the registry's current state
     * (the model that's already serving), adds resolve from {@code newConfig}, and modifies
     * resolve <em>both</em> — old from the registry, new from the submitted configuration.
     *
     * <p>Registry-side resolution uses {@link VirtualClusterRegistry#modelFor(String)} —
     * a direct name-keyed lookup to fetch the old existing model.
     * The new-config side still requires a Map-build because
     * {@code modelResolver} returns a list (it has no direct name-lookup affordance).
     *
     * @throws IllegalStateException if any of {@code clustersToRemove}, {@code clustersToAdd},
     *         or {@code clustersToModify} names a cluster that can't be resolved on the side
     *         it's expected — all three indicate a {@code ChangeDetector} contract violation
     *         (i.e. a framework bug).
     */
    List<ClusterOperation> plan(ChangeResult changes, Configuration newConfig) {
        var ops = new ArrayList<ClusterOperation>();

        // The new-config side needs a name → model lookup.
        Map<String, VirtualClusterModel> newModelsByName = (changes.clustersToAdd().isEmpty()
                && changes.clustersToModify().isEmpty()) ? Map.of() : resolveByName(newConfig);

        for (String name : changes.clustersToRemove()) {
            VirtualClusterModel oldModel = virtualClusterRegistry.modelFor(name);
            if (oldModel == null) {
                throw new IllegalStateException(
                        "OperationsPlanner: no model for removed cluster '" + name
                                + "'; this indicates a ChangeDetector contract violation"
                                + " (cluster reported as removed but absent from the registry)");
            }
            ops.add(new RemoveCluster(oldModel, virtualClusterRegistry, endpointRegistry));
        }

        for (String name : changes.clustersToModify()) {
            VirtualClusterModel oldModel = virtualClusterRegistry.modelFor(name);
            if (oldModel == null) {
                throw new IllegalStateException(
                        "OperationsPlanner: no old model for modified cluster '" + name
                                + "'; this indicates a ChangeDetector contract violation"
                                + " (cluster reported as modified but absent from the registry)");
            }
            VirtualClusterModel newModel = newModelsByName.get(name);
            if (newModel == null) {
                throw new IllegalStateException(
                        "OperationsPlanner: no new model for modified cluster '" + name
                                + "'; this indicates a ChangeDetector contract violation"
                                + " (cluster reported as modified but absent from the submitted configuration)");
            }
            ops.add(new ReplaceCluster(oldModel, newModel, virtualClusterRegistry, endpointRegistry));
        }

        for (String name : changes.clustersToAdd()) {
            VirtualClusterModel newModel = newModelsByName.get(name);
            if (newModel == null) {
                throw new IllegalStateException(
                        "OperationsPlanner: no model for added cluster '" + name
                                + "'; this indicates a ChangeDetector contract violation"
                                + " (cluster reported as added but absent from the submitted configuration)");
            }
            ops.add(new AddCluster(newModel, virtualClusterRegistry, endpointRegistry));
        }

        return List.copyOf(ops);
    }

    private Map<String, VirtualClusterModel> resolveByName(Configuration newConfig) {
        return modelResolver.apply(newConfig).stream()
                .collect(Collectors.toUnmodifiableMap(VirtualClusterModel::getClusterName, Function.identity()));
    }
}
