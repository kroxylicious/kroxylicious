/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Optional;

import io.kroxylicious.proxy.reload.ReconfigureError;

/**
 * A single per-virtual-cluster operation applied during a {@code reconfigure()}. Sealed so
 * the orchestrator can reason exhaustively about every kind of operation a planner can
 * produce — adding a new operation type is a deliberate change that the compiler can flag
 * at every dispatch point.
 */
sealed interface ClusterOperation permits AddCluster, RemoveCluster, ReplaceCluster {

    /**
     * The kind of change this operation applies to a virtual cluster. The {@link #label()} is
     * used verbatim as the {@code operation} metric tag value.
     */
    enum Operation {
        ADD("add"),
        REMOVE("remove"),
        MODIFY("modify");

        private final String label;

        Operation(String label) {
            this.label = label;
        }

        public String label() {
            return label;
        }
    }

    String clusterName();

    /**
     * The kind of change this operation represents, for observability.
     */
    Operation operation();

    /**
     * Apply this operation. Returns an error if the operation didn't complete cleanly;
     * empty on full success. Implementations must not throw — failures are reported via
     * the return value so the orchestrator can continue with subsequent operations.
     */
    Optional<ReconfigureError> apply();
}
