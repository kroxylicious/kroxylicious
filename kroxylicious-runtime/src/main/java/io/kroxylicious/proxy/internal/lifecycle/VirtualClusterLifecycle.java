/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.lifecycle;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.DRAINING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.FAILED;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.INITIALIZING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.SERVING;
import static io.kroxylicious.proxy.internal.lifecycle.VirtualClusterLifecycleState.STOPPED;

/**
 * Holds the lifecycle state for a single virtual cluster. Thread-safe: state reads are
 * lock-free via volatile; state writes are synchronized and validate legal transitions.
 */
public class VirtualClusterLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterLifecycle.class);

    private static final Map<VirtualClusterLifecycleState, Set<VirtualClusterLifecycleState>> LEGAL_TRANSITIONS = Map.of(
            INITIALIZING, Set.of(SERVING, FAILED),
            SERVING, Set.of(DRAINING),
            DRAINING, Set.of(STOPPED, INITIALIZING),
            FAILED, Set.of(STOPPED, INITIALIZING));

    private final String clusterName;
    private volatile VirtualClusterLifecycleState state;
    @Nullable
    private volatile Throwable failureCause;

    VirtualClusterLifecycle(String clusterName) {
        this.clusterName = clusterName;
        this.state = INITIALIZING;
    }

    /**
     * Returns the virtual cluster name.
     */
    public String clusterName() {
        return clusterName;
    }

    /**
     * Returns the current lifecycle state. Lock-free volatile read.
     */
    public VirtualClusterLifecycleState state() {
        return state;
    }

    /**
     * Returns {@code true} if the virtual cluster is in the {@link VirtualClusterLifecycleState#SERVING} state.
     */
    public boolean isAcceptingConnections() {
        return state == SERVING;
    }

    /**
     * Returns the failure cause if this lifecycle is in the {@link VirtualClusterLifecycleState#FAILED} state.
     */
    public Optional<Throwable> failureCause() {
        return Optional.ofNullable(failureCause);
    }

    /**
     * Transitions this lifecycle to the given target state.
     * Validates that the transition is legal and throws {@link IllegalStateException} if not.
     *
     * @param target the target state
     * @throws IllegalStateException if the transition is not legal
     */
    public synchronized void transitionTo(VirtualClusterLifecycleState target) {
        VirtualClusterLifecycleState current = this.state;
        Set<VirtualClusterLifecycleState> allowed = LEGAL_TRANSITIONS.getOrDefault(current, Set.of());
        if (!allowed.contains(target)) {
            throw new IllegalStateException(
                    "Virtual cluster '%s': illegal transition %s -> %s (allowed: %s)".formatted(clusterName, current, target, allowed));
        }
        LOGGER.info("Virtual cluster '{}': {} -> {}", clusterName, current, target);
        this.failureCause = null;
        this.state = target;
    }

    /**
     * Transitions this lifecycle to the {@link VirtualClusterLifecycleState#FAILED} state, storing the cause.
     *
     * @param cause the failure cause
     * @throws IllegalStateException if the current state does not allow transitioning to FAILED
     */
    public synchronized void transitionToFailed(Throwable cause) {
        VirtualClusterLifecycleState current = this.state;
        Set<VirtualClusterLifecycleState> allowed = LEGAL_TRANSITIONS.getOrDefault(current, Set.of());
        if (!allowed.contains(FAILED)) {
            throw new IllegalStateException(
                    "Virtual cluster '%s': illegal transition %s -> FAILED (allowed: %s)".formatted(clusterName, current, allowed));
        }
        LOGGER.atError()
                .setCause(LOGGER.isDebugEnabled() ? cause : null)
                .log("Virtual cluster '{}': {} -> FAILED: {}. Increase log level to DEBUG for stacktrace", clusterName, current, cause.getMessage());
        this.failureCause = cause;
        this.state = FAILED;
    }
}
