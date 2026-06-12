/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;
import io.kroxylicious.proxy.reload.ReconfigureError;

/**
 * Apply a configuration change to an existing virtual cluster during a reconfigure.
 *
 * <h2>Remove then add</h2>
 * Today's implementation is the simplest possible: drive {@link RemoveCluster} on {@code oldModel}
 * to drain the existing serving cluster, then {@link AddCluster} on {@code newModel} to bring the
 * new shape online. Composition rather than reimplementation: each sub-operation already owns its
 * own failure semantics, rollback, and lifecycle transitions, so this class is just a sequencer.
 *
 * <h2>Failure semantics</h2>
 * <ul>
 *   <li><b>Remove half fails</b> ⇒ the add half is <em>skipped</em>. The cluster is left in
 *       whatever terminal state the remove failure produced. The returned {@link ReconfigureError}
 *       attributes the failure to a single phase, so the orchestrator surfaces one error per
 *       failing cluster — not a remove-error plus a phantom add-error.</li>
 *   <li><b>Add half fails after remove succeeds</b> ⇒ the cluster ends up offline (entry exists in
 *       the registry but the lifecycle is {@code STOPPED}). The {@link ReconfigureError} carries
 *       the bind cause from the add half. This is consistent with the failure shape of a pure
 *       {@link AddCluster}, so operator-facing semantics are predictable.</li>
 * </ul>
 */
final class ReplaceCluster implements ClusterOperation {

    private final VirtualClusterModel oldModel;
    private final String clusterName;
    private final Supplier<VirtualClusterModel> newModelSupplier;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;

    ReplaceCluster(VirtualClusterModel oldModel,
                   String clusterName,
                   Supplier<VirtualClusterModel> newModelSupplier,
                   VirtualClusterRegistry virtualClusterRegistry,
                   EndpointRegistry endpointRegistry) {
        this.oldModel = Objects.requireNonNull(oldModel, "oldModel");
        this.clusterName = Objects.requireNonNull(clusterName, "clusterName");
        this.newModelSupplier = Objects.requireNonNull(newModelSupplier, "newModelSupplier");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        // The ChangeDetector contract guarantees the old side's cluster name matches the planned
        // cluster name; guard it locally to catch a buggy planner / detector early.
        if (!oldModel.getClusterName().equals(clusterName)) {
            throw new IllegalArgumentException(
                    "ReplaceCluster: oldModel cluster name '" + oldModel.getClusterName()
                            + "' does not match planned cluster name '" + clusterName + "'");
        }
    }

    @Override
    public String clusterName() {
        return clusterName;
    }

    @Override
    public Optional<ReconfigureError> apply() {
        var removeError = new RemoveCluster(oldModel, virtualClusterRegistry, endpointRegistry).apply();
        if (removeError.isPresent()) {
            // Short-circuit: don't attempt the add half. We want one error per failing cluster,
            // not a cascade where a stuck-on-remove cluster also produces a (likely unrelated) add
            // failure. The cluster stays in the failure state RemoveCluster left it in.
            return removeError;
        }
        return new AddCluster(clusterName, newModelSupplier, virtualClusterRegistry, endpointRegistry).apply();
    }
}
