/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Draining;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Failed;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Initializing;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the lifecycle state of a single virtual cluster.
 * <p>
 * Thread-safe: state transitions and connection registration share the same monitor,
 * so the state transition → connection snapshot sequence in {@link #startDraining()}
 * is atomic with respect to {@link #registerConnection(ProxyChannelStateMachine)}.
 * This closes the TOCTOU window where a connection could be registered after the
 * drain snapshot is taken and therefore missed by graceful drain.
 * </p>
 */
public class VirtualClusterLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterLifecycle.class);

    private final String clusterName;
    private final Duration drainTimeout;
    private VirtualClusterLifecycleState state = new Initializing();
    private final Set<ProxyChannelStateMachine> activeConnections = new HashSet<>();
    @Nullable
    private CompletableFuture<Void> drainFuture;

    public VirtualClusterLifecycle(String clusterName, Duration drainTimeout) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.drainTimeout = drainTimeout;
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
     * Transitions from {@link Serving} to {@link Draining} and initiates close on all
     * active connections.
     *
     * @return future that completes when all connections have closed
     */
    public CompletableFuture<Void> startDraining() {
        List<ProxyChannelStateMachine> snapshot;
        synchronized (this) {
            transition(current -> {
                if (current instanceof Serving s) {
                    return s.toDraining(drainTimeout);
                }
                throw unexpectedState(current, "startDraining");
            });
            snapshot = List.copyOf(activeConnections);
        }
        var closeFutures = snapshot.stream()
                .map(pcsm -> pcsm.drain(drainTimeout))
                .toArray(CompletableFuture[]::new);
        drainFuture = CompletableFuture.allOf(closeFutures);
        return drainFuture;
    }

    /**
     * Returns the future that completes when all connections have drained.
     * Only valid to call when the cluster is in {@link Draining} state.
     */
    public CompletableFuture<Void> drainFuture() {
        return Objects.requireNonNull(drainFuture, "drainFuture is only set after startDraining()");
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

    public synchronized VirtualClusterLifecycleState state() {
        return state;
    }

    public String clusterName() {
        return clusterName;
    }

    public synchronized boolean registerConnection(ProxyChannelStateMachine pcsm) {
        if (state instanceof Draining || state instanceof Stopped) {
            return false;
        }
        activeConnections.add(pcsm);
        return true;
    }

    public synchronized void deregisterConnection(ProxyChannelStateMachine pcsm) {
        activeConnections.remove(pcsm);
    }

    public Set<ProxyChannelStateMachine> activeConnections() {
        return Set.copyOf(activeConnections);
    }

    private synchronized void transition(UnaryOperator<VirtualClusterLifecycleState> transitionFn) {
        VirtualClusterLifecycleState previous = state;
        state = transitionFn.apply(state);
        var logBuilder = (state instanceof Serving || state instanceof Failed || state instanceof Stopped) ? LOGGER.atInfo() : LOGGER.atDebug();
        logBuilder
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("from", () -> previous.getClass().getSimpleName())
                .addKeyValue("to", () -> state.getClass().getSimpleName())
                .log("Virtual cluster lifecycle transition");
    }

    private IllegalStateException unexpectedState(VirtualClusterLifecycleState current, String operation) {
        return new IllegalStateException(
                "Cannot " + operation + " for virtual cluster '" + clusterName + "' in state " + current.getClass().getSimpleName());
    }
}
