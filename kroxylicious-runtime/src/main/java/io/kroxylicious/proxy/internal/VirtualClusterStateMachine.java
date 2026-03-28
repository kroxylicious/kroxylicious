/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.kroxylicious.proxy.internal.VirtualClusterState.DRAINING;
import static io.kroxylicious.proxy.internal.VirtualClusterState.FAILED;
import static io.kroxylicious.proxy.internal.VirtualClusterState.INITIALIZING;
import static io.kroxylicious.proxy.internal.VirtualClusterState.SERVING;
import static io.kroxylicious.proxy.internal.VirtualClusterState.STOPPED;

/**
 * Tracks the lifecycle state of a virtual cluster and enforces valid state transitions.
 * <p>
 * Valid transitions:
 * <pre>
 *   INITIALIZING → SERVING      (startup/reload success)
 *   INITIALIZING → FAILED       (startup/reload failure)
 *   SERVING      → DRAINING     (shutdown or structural reload)
 *   DRAINING     → INITIALIZING (reload: re-init after drain)
 *   DRAINING     → STOPPED      (shutdown or cluster removal — terminal)
 *   FAILED       → INITIALIZING (retry with corrected config)
 *   FAILED       → STOPPED      (shutdown or cluster removal — terminal)
 * </pre>
 * <p>
 * This class is thread-safe.
 */
public class VirtualClusterStateMachine {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterStateMachine.class);

    private static final Map<VirtualClusterState, Set<VirtualClusterState>> VALID_TRANSITIONS = Map.of(
            INITIALIZING, Set.of(SERVING, FAILED),
            SERVING, Set.of(DRAINING),
            DRAINING, Set.of(INITIALIZING, STOPPED),
            FAILED, Set.of(INITIALIZING, STOPPED),
            STOPPED, Set.of());

    private final String clusterName;
    private volatile VirtualClusterState currentState;

    /**
     * Creates a new lifecycle tracker starting in {@link VirtualClusterState#INITIALIZING}.
     *
     * @param clusterName the name of the virtual cluster, used in log and error messages
     */
    public VirtualClusterStateMachine(String clusterName) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.currentState = INITIALIZING;
    }

    /**
     * Transitions to the given state if the transition is valid.
     *
     * @param newState the target state
     * @throws IllegalStateException if the transition is not valid
     */
    public synchronized void transitionTo(VirtualClusterState newState) {
        Objects.requireNonNull(newState);
        var allowed = VALID_TRANSITIONS.get(currentState);
        if (allowed == null || !allowed.contains(newState)) {
            throw new IllegalStateException(
                    "Virtual cluster '%s': invalid lifecycle transition %s → %s".formatted(clusterName, currentState, newState));
        }
        LOGGER.info("Virtual cluster '{}': {} → {}", clusterName, currentState, newState);
        currentState = newState;
    }

    /**
     * Returns the current lifecycle state.
     *
     * @return the current state
     */
    public VirtualClusterState currentState() {
        return currentState;
    }

    /**
     * Returns the cluster name associated with this lifecycle.
     *
     * @return the cluster name
     */
    public String clusterName() {
        return clusterName;
    }
}