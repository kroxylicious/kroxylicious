/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Lifecycle states for a virtual cluster.
 * <p>
 * Each virtual cluster has exactly one state at any time. States are immutable
 * records; transitions produce a new state instance.
 * </p>
 *
 * @see <a href="https://github.com/kroxylicious/design/blob/main/proposals/016-virtual-cluster-lifecycle.md">Proposal 016</a>
 */
public sealed interface VirtualClusterLifecycleState {

    /** The cluster is being set up. Not yet serving traffic. */
    record Initializing() implements VirtualClusterLifecycleState {

        /**
         * Configuration applied successfully.
         * @return the new serving state
         */
        public Serving toServing() {
            return new Serving();
        }

        /**
         * Configuration could not be applied.
         * @param cause the failure reason
         * @return the new failed state
         */
        public Failed toFailed(Throwable cause) {
            return new Failed(cause);
        }
    }

    /** The proxy has completed setup and is serving traffic for this cluster. */
    record Serving() implements VirtualClusterLifecycleState {

        /**
         * The cluster is being shut down or reconfigured.
         * @return the new draining state
         */
        public Draining toDraining() {
            return new Draining();
        }
    }

    /** New connections are rejected. Existing in-flight requests are completing. */
    record Draining() implements VirtualClusterLifecycleState {

        /**
         * Drain complete, cluster permanently removed.
         * @return the new stopped state
         */
        public Stopped toStopped() {
            return new Stopped(null);
        }

        /**
         * Drain complete, cluster reinitialising with new configuration.
         * @return the new initializing state
         */
        public Initializing toInitializing() {
            return new Initializing();
        }
    }

    /** Configuration was not viable. All resources have been released. */
    record Failed(Throwable cause) implements VirtualClusterLifecycleState {

        public Failed {
            Objects.requireNonNull(cause);
        }

        /**
         * The cluster is being permanently removed.
         * @return the new stopped state, retaining the failure cause for diagnostics
         */
        public Stopped toStopped() {
            return new Stopped(cause);
        }

        /**
         * A retry is requested with corrected configuration.
         * @return the new initializing state
         */
        public Initializing toInitializing() {
            return new Initializing();
        }
    }

    /** Terminal state. The cluster has been permanently removed. */
    record Stopped(@Nullable Throwable priorFailureCause) implements VirtualClusterLifecycleState {}
}
