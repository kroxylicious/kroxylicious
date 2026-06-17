/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Timer;

import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Draining;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Failed;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Initializing;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;
import io.kroxylicious.proxy.internal.util.Metrics;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Manages the lifecycle state of a single virtual cluster.
 * <p>
 * Thread-safe: state transitions and connection registration share the same monitor,
 * so the state transition → connection snapshot sequence in {@link #startDraining()}
 * is atomic with respect to {@link #registerConnection(ClientConnectionStateMachine)}.
 * This closes the TOCTOU window where a connection could be registered after the
 * drain snapshot is taken and therefore missed by graceful drain.
 * </p>
 */
public class VirtualClusterLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualClusterLifecycle.class);

    private final String clusterName;
    private final Duration drainTimeout;
    private final Clock clock;
    private VirtualClusterLifecycleState state = new Initializing();
    private final Set<ClientConnectionStateMachine> activeConnections = new HashSet<>();
    /**
     * Assigned in {@link #startDraining()} after the synchronized state transition, then read by
     * {@link #drainFuture()}. {@code volatile} so a reader that observes {@code Draining} via
     * the synchronized {@link #state()} getter always sees the write (the synchronized state
     * read alone gives happens-before with writes inside the synchronized block but not with
     * this write, which is intentionally outside the lock to keep the lock window tight).
     */
    @SuppressWarnings("java:S3077")
    @Nullable
    private volatile CompletableFuture<Void> drainFuture;

    /** When the current {@link #state} was entered; used to time {@code state_duration}. */
    private final AtomicLong stateEnteredNanos;

    public VirtualClusterLifecycle(String clusterName, Duration drainTimeout) {
        this(clusterName, drainTimeout, Clock.SYSTEM);
    }

    /**
     * Test seam: injects the {@link Clock} used to time drains and per-state durations, so tests
     * can drive a {@link io.micrometer.core.instrument.MockClock} and assert recorded durations
     * deterministically. Production always uses {@link Clock#SYSTEM}.
     */
    @VisibleForTesting
    VirtualClusterLifecycle(String clusterName, Duration drainTimeout, Clock clock) {
        this.clusterName = Objects.requireNonNull(clusterName);
        this.drainTimeout = drainTimeout;
        this.clock = Objects.requireNonNull(clock);
        this.stateEnteredNanos = new AtomicLong(clock.monotonicTime());
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
        List<ClientConnectionStateMachine> snapshot;
        Timer.Sample drainSample;
        synchronized (this) {
            transition(current -> {
                if (current instanceof Serving s) {
                    return s.toDraining(drainTimeout);
                }
                throw unexpectedState(current, "startDraining");
            });
            snapshot = List.copyOf(activeConnections);
            drainSample = Timer.start(clock);
        }
        var closeFutures = snapshot.stream()
                .map(ccsm -> ccsm.drain(drainTimeout))
                .toArray(CompletableFuture[]::new);
        drainFuture = CompletableFuture.allOf(closeFutures);
        drainFuture.whenComplete((v, t) -> drainSample.stop(Metrics.drainDurationTimer(clusterName)));
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
     * Transitions to {@link Stopped} from {@link Failed} or {@link Initializing}. Idempotent:
     * a call when already {@link Stopped} is a silent no-op, so two concurrent shutdown paths
     * that both observe a non-terminal state and race to call {@code stop()} cannot fail —
     * the second caller's transition simply finds the state already terminal.
     */
    public void stop() {
        transition(current -> {
            if (current instanceof Stopped) {
                return current;
            }
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

    public synchronized boolean registerConnection(ClientConnectionStateMachine ccsm) {
        if (!(state instanceof Serving)) {
            return false;
        }
        activeConnections.add(ccsm);
        return true;
    }

    public synchronized void deregisterConnection(ClientConnectionStateMachine ccsm) {
        activeConnections.remove(ccsm);
    }

    public Set<ClientConnectionStateMachine> activeConnections() {
        return Set.copyOf(activeConnections);
    }

    private synchronized void transition(UnaryOperator<VirtualClusterLifecycleState> transitionFn) {
        VirtualClusterLifecycleState previous = state;
        state = transitionFn.apply(state);
        // An idempotent no-op (e.g. stop() called when already Stopped) returns the same
        // instance — don't record a phantom self-transition.
        if (state == previous) {
            return;
        }
        recordTransitionMetrics(previous, state);
        var logBuilder = (state instanceof Serving || state instanceof Failed || state instanceof Stopped) ? LOGGER.atInfo() : LOGGER.atDebug();
        logBuilder
                .addKeyValue("virtualCluster", clusterName)
                .addKeyValue("from", () -> previous.getClass().getSimpleName())
                .addKeyValue("to", () -> state.getClass().getSimpleName())
                .log("Virtual cluster lifecycle transition");
    }

    private void recordTransitionMetrics(VirtualClusterLifecycleState from, VirtualClusterLifecycleState to) {
        long now = clock.monotonicTime();
        long previousEntry = stateEnteredNanos.getAndSet(now);
        String fromLabel = stateLabel(from);
        Metrics.virtualClusterStateDurationTimer(clusterName, fromLabel).record(Duration.ofNanos(now - previousEntry));
        Metrics.virtualClusterTransitionsCounter(clusterName, fromLabel, stateLabel(to)).increment();
        Metrics.updateVirtualClusterState(clusterName, stateLabel(to));
    }

    private static String stateLabel(VirtualClusterLifecycleState state) {
        return state.getClass().getSimpleName().toLowerCase(Locale.ROOT);
    }

    private IllegalStateException unexpectedState(VirtualClusterLifecycleState current, String operation) {
        return new IllegalStateException(
                "Cannot " + operation + " for virtual cluster '" + clusterName + "' in state " + current.getClass().getSimpleName());
    }
}
