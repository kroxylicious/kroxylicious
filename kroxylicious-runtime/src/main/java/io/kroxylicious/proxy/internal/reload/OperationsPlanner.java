/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

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
    private final BiFunction<Configuration, String, VirtualClusterModel> modelResolver;

    OperationsPlanner(VirtualClusterRegistry virtualClusterRegistry,
                      EndpointRegistry endpointRegistry,
                      BiFunction<Configuration, String, VirtualClusterModel> modelResolver) {
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        this.modelResolver = Objects.requireNonNull(modelResolver, "modelResolver");
    }

    /**
     * Plan the operations for a reconfigure. Operations that need a model from the new
     * configuration (adds, modifies) receive a lazy supplier rather than an eagerly-built
     * {@link VirtualClusterModel}: the model is constructed inside the operation's
     * {@code apply()} when needed, not during planning.
     *
     * <p>Lazy construction matters because building a VCM eagerly calls every filter's
     * {@code initialize()}. Building VCMs for clusters that the planner doesn't touch
     * leaks initialisations that are never matched by a {@code close()}.
     * With lazy construction, only clusters that actually receive an Add or Replace
     * operation are constructed.
     *
     * <p>Registry-side resolution uses {@link VirtualClusterRegistry#modelFor(String)} —
     * a direct name-keyed lookup to fetch the old existing model (used by removes and the
     * old side of replaces).
     *
     * @throws IllegalStateException if any of {@code clustersToRemove} or
     *         {@code clustersToModify} names a cluster that can't be resolved against the
     *         registry — both indicate a {@code ChangeDetector} contract violation
     *         (i.e. a framework bug).
     */
    List<ClusterOperation> plan(ChangeResult changes, Configuration newConfig) {
        var ops = new ArrayList<ClusterOperation>();

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
            requireClusterInConfig(newConfig, name, "modified");
            ops.add(new ReplaceCluster(oldModel, name, supplierFor(newConfig, name), virtualClusterRegistry, endpointRegistry));
        }

        for (String name : changes.clustersToAdd()) {
            requireClusterInConfig(newConfig, name, "added");
            ops.add(new AddCluster(name, supplierFor(newConfig, name), virtualClusterRegistry, endpointRegistry));
        }

        return List.copyOf(ops);
    }

    /**
     * Eager phantom-cluster check. Verifies the named cluster exists in the submitted
     * configuration without triggering VCM construction (and therefore without running any
     * filter's {@code initialize()}). Preserves the "loud at framework layer" diagnostic for
     * a buggy {@code ChangeDetector} that names a cluster absent from the submitted config.
     */
    private static void requireClusterInConfig(Configuration newConfig, String clusterName, String operation) {
        boolean present = newConfig.virtualClusters().stream()
                .anyMatch(vc -> vc.name().equals(clusterName));
        if (!present) {
            throw new IllegalStateException(
                    "OperationsPlanner: no model for " + operation + " cluster '" + clusterName
                            + "'; this indicates a ChangeDetector contract violation"
                            + " (cluster reported as " + operation + " but absent from the submitted configuration)");
        }
    }

    private Supplier<VirtualClusterModel> supplierFor(Configuration newConfig, String clusterName) {
        return () -> modelResolver.apply(newConfig, clusterName);
    }
}
