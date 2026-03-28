/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

/**
 * Describes the possible lifecycle states of a virtual cluster.
 * <p>
 * A virtual cluster transitions through these states during its lifetime:
 * <ul>
 *   <li>{@code INITIALIZING → SERVING} on successful startup</li>
 *   <li>{@code INITIALIZING → FAILED} on startup failure</li>
 *   <li>{@code SERVING → DRAINING} on shutdown or before structural reload</li>
 *   <li>{@code DRAINING → INITIALIZING} on reload (re-init after drain)</li>
 *   <li>{@code DRAINING → STOPPED} on shutdown or cluster removal (terminal)</li>
 *   <li>{@code FAILED → INITIALIZING} on retry with corrected config</li>
 *   <li>{@code FAILED → STOPPED} on shutdown or cluster removal (terminal)</li>
 * </ul>
 */
public enum VirtualClusterState {
    /**
     * The cluster is being set up. Not yet serving traffic.
     * Used on first boot, when retrying from {@link #FAILED}, and during configuration reload.
     */
    INITIALIZING,
    /**
     * The proxy has completed setup for this cluster and is serving traffic.
     * This state makes no claim about the availability of upstream brokers — it means
     * the proxy is ready to handle connection attempts.
     */
    SERVING,
    /**
     * New connections are rejected. Existing in-flight requests are given the opportunity
     * to complete. Connections are closed once drained or the drain timeout is reached.
     */
    DRAINING,
    /**
     * The proxy determined the configuration not to be viable. All partially-acquired
     * resources are released on entry to this state.
     */
    FAILED,
    /**
     * The cluster has been permanently removed from the configuration or the proxy is
     * shutting down. All resources have been released. This is a terminal state.
     */
    STOPPED
}