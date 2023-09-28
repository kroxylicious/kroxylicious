/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.enums;

/**
 * The enum Custom resource status.
 */
public enum CustomResourceStatus {
    /**
     *Ready custom resource status.
     */
    Ready,
    /**
     *Not ready custom resource status.
     */
    NotReady,
    /**
     *Warning custom resource status.
     */
    Warning,
    /**
     *Reconciliation paused custom resource status.
     */
    ReconciliationPaused
}
