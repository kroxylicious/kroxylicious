/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Draining;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Failed;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Initializing;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;

/**
 * Manages the lifecycle state of a single virtual cluster.
 * <p>
 * Thread-safe: state transitions are performed atomically via {@link AtomicReference}.
 * </p>
 */
public class VirtualClusterLifecycleManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterLifecycleManager.class);

    private final String clusterName;
    private final AtomicReference<VirtualClusterLifecycleState> state = new AtomicReference<>(new Initializing());

    public VirtualClusterLifecycleManager(String clusterName) {
        this.clusterName = Objects.requireNonNull(clusterName);
    }

    /**
     * Transitions from {@link Initializing} to {@link Serving}.
     */
    public void initializationSucceeded() {
        transition(current -> {
            if (current instanceof Initializing s) {
                return s.toServing();
            }
            throw unexpectedState(current, "initializationSucceeded");
        });
    }

    /**
     * Transitions from {@link Initializing} to {@link Failed}, recording the cause.
     */
    public void initializationFailed(Throwable cause) {
        Objects.requireNonNull(cause);
        transition(current -> {
            if (current instanceof Initializing s) {
                return s.toFailed(cause);
            }
            throw unexpectedState(current, "initializationFailed");
        });
    }

    /**
     * Transitions from {@link Serving} to {@link Draining}.
     */
    public void startDraining() {
        transition(current -> {
            if (current instanceof Serving s) {
                return s.toDraining();
            }
            throw unexpectedState(current, "startDraining");
        });
    }

    /**
     * Transitions from {@link Draining} to {@link Stopped}.
     */
    public void drainComplete() {
        transition(current -> {
            if (current instanceof Draining s) {
                return s.toStopped();
            }
            throw unexpectedState(current, "drainComplete");
        });
    }

    /**
     * Transitions to {@link Stopped} from {@link Failed} or {@link Initializing}.
     */
    public void stop() {
        transition(current -> {
            if (current instanceof Failed s) {
                return s.toStopped();
            }
            if (current instanceof Initializing s) {
                return s.toStopped();
            }
            throw unexpectedState(current, "stop");
        });
    }

    public VirtualClusterLifecycleState getState() {
        return state.get();
    }

    public String getClusterName() {
        return clusterName;
    }

    private void transition(UnaryOperator<VirtualClusterLifecycleState> transitionFn) {
        // Use get() then updateAndGet() rather than getAndUpdate() so we capture
        // both the previous and new state without re-applying the transition function.
        VirtualClusterLifecycleState previous = state.get();
        VirtualClusterLifecycleState current = state.updateAndGet(transitionFn);
        var logBuilder = (current instanceof Failed) ? LOGGER.atWarn() : LOGGER.atInfo();
        logBuilder
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("from", previous.getClass().getSimpleName())
                .addKeyValue("to", current.getClass().getSimpleName())
                .log("Virtual cluster lifecycle transition");
    }

    private IllegalStateException unexpectedState(VirtualClusterLifecycleState current, String operation) {
        return new IllegalStateException(
                "Cannot " + operation + " for virtual cluster '" + clusterName + "' in state " + current.getClass().getSimpleName());
    }
}
