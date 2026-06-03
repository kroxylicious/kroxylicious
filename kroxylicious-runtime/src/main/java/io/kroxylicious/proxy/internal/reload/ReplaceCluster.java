/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Objects;
import java.util.Optional;

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
    private final VirtualClusterModel newModel;
    private final VirtualClusterRegistry virtualClusterRegistry;
    private final EndpointRegistry endpointRegistry;

    ReplaceCluster(VirtualClusterModel oldModel,
                   VirtualClusterModel newModel,
                   VirtualClusterRegistry virtualClusterRegistry,
                   EndpointRegistry endpointRegistry) {
        this.oldModel = Objects.requireNonNull(oldModel, "oldModel");
        this.newModel = Objects.requireNonNull(newModel, "newModel");
        this.virtualClusterRegistry = Objects.requireNonNull(virtualClusterRegistry, "virtualClusterRegistry");
        this.endpointRegistry = Objects.requireNonNull(endpointRegistry, "endpointRegistry");
        // The ChangeDetector contract guarantees both sides share the cluster name; we guard it
        // locally to catch a buggy planner / detector before it produces a corrupted reconfigure.
        if (!oldModel.getClusterName().equals(newModel.getClusterName())) {
            throw new IllegalArgumentException(
                    "ReplaceCluster requires the same cluster name on both sides; got '"
                            + oldModel.getClusterName() + "' (old) vs '" + newModel.getClusterName() + "' (new)");
        }
    }

    @Override
    public String clusterName() {
        return newModel.getClusterName();
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
        return new AddCluster(newModel, virtualClusterRegistry, endpointRegistry).apply();
    }
}
