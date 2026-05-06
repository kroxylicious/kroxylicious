/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Draining;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Failed;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Initializing;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Serving;
import io.kroxylicious.proxy.internal.VirtualClusterLifecycleState.Stopped;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;
import static org.mockito.Mockito.mock;

class VirtualClusterLifecycleTest {

    private static final String CLUSTER_NAME = "test-cluster";
    private static final Duration DRAIN_TIMEOUT = Duration.ofSeconds(5);
    private VirtualClusterLifecycle manager;

    @BeforeEach
    void setUp() {
        manager = new VirtualClusterLifecycle(CLUSTER_NAME, DRAIN_TIMEOUT);
    }

    @Test
    void shouldStartInInitializingState() {
        assertThat(manager.state()).isInstanceOf(Initializing.class);
    }

    @Test
    void shouldExposeClusterName() {
        assertThat(manager.clusterName()).isEqualTo(CLUSTER_NAME);
    }

    @Test
    void shouldTransitionToServingOnSuccess() {
        // when
        manager.initializationSucceeded();

        // then
        assertThat(manager.state()).isInstanceOf(Serving.class);
    }

    @Test
    void shouldTransitionToFailedOnError() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        manager.initializationFailed(cause);

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Failed.class, failed -> assertThat(failed.cause()).isSameAs(cause));
    }

    @Test
    void shouldTransitionFromServingToDraining() {
        // given
        manager.initializationSucceeded();

        // when
        manager.startDraining();

        // then
        VirtualClusterLifecycleState state = manager.state();
        assertThat(state).asInstanceOf(InstanceOfAssertFactories.type(Draining.class))
                .satisfies(draining -> assertThat(draining.drainTimeout()).isEqualTo(DRAIN_TIMEOUT));
    }

    @Test
    void shouldTransitionFromDrainingToStopped() {
        // given
        manager.initializationSucceeded();
        manager.startDraining();

        // when
        manager.drainComplete();

        // then
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void shouldTransitionFromFailedToStopped() {
        // given
        manager.initializationFailed(new RuntimeException("boom"));

        // when
        manager.stop();

        // then
        assertThat(manager.state()).isInstanceOf(Stopped.class);
    }

    @Test
    void shouldRetainFailureCauseAfterStop() {
        // given
        var cause = new RuntimeException("boom");
        manager.initializationFailed(cause);

        // when
        manager.stop();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isSameAs(cause));
    }

    @Test
    void shouldTransitionFromInitializingToStoppedOnShutdown() {
        // when
        manager.stop();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isNull());
    }

    @Test
    void shouldHaveNoPriorFailureCauseWhenStoppedFromDraining() {
        // given
        manager.initializationSucceeded();
        manager.startDraining();

        // when
        manager.drainComplete();

        // then
        assertThat(manager.state())
                .isInstanceOfSatisfying(Stopped.class, stopped -> assertThat(stopped.priorFailureCause()).isNull());
    }

    static Stream<Arguments> invalidTransitions() {
        return Stream.of(
                argumentSet("initializationSucceeded from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.initializationSucceeded();
                }),
                argumentSet("startDraining from INITIALIZING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.startDraining();
                }),
                argumentSet("drainComplete from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.drainComplete();
                }),
                argumentSet("stop from SERVING", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationSucceeded();
                    m.stop();
                }),
                argumentSet("initializationSucceeded from STOPPED", (Runnable) () -> {
                    var m = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
                    m.initializationFailed(new RuntimeException("x"));
                    m.stop();
                    m.initializationSucceeded();
                }));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidTransitions")
    void shouldRejectInvalidTransition(Runnable action) {
        assertThatThrownBy(action::run)
                .isInstanceOf(IllegalStateException.class);
    }

    // --- Concurrency ---

    @Test
    void concurrentRegisterAndDeregisterDoesNotLoseConnection() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            var lifecycle = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
            var pcsm1 = mock(ProxyChannelStateMachine.class);
            var pcsm2 = mock(ProxyChannelStateMachine.class);
            lifecycle.registerConnection(pcsm1);

            var startGate = new CountDownLatch(1);
            var deregisterer = new Thread(() -> {
                awaitGate(startGate);
                lifecycle.deregisterConnection(pcsm1);
            });
            var registerer = new Thread(() -> {
                awaitGate(startGate);
                lifecycle.registerConnection(pcsm2);
            });
            deregisterer.start();
            registerer.start();
            startGate.countDown();
            deregisterer.join();
            registerer.join();

            assertThat(lifecycle.activeConnections())
                    .as("iteration %d: pcsm2 must still be registered after concurrent register/deregister", iter)
                    .containsExactly(pcsm2);
        }
    }

    /**
     * Higher-volume contention test. Each thread registers a batch of distinct PCSMs and
     * deregisters half of them. The expected end state is exactly the never-deregistered
     * PCSMs from every thread — no PCSM lost, no ghost PCSM remaining.
     */
    @Test
    void registerAndDeregisterAreThreadSafeUnderContention() throws Exception {
        int threads = 8;
        int operationsPerThread = 500;
        var lifecycle = new VirtualClusterLifecycle("c", DRAIN_TIMEOUT);
        var pcsms = new ProxyChannelStateMachine[threads * operationsPerThread];
        for (int i = 0; i < pcsms.length; i++) {
            pcsms[i] = mock(ProxyChannelStateMachine.class);
        }

        var executor = Executors.newFixedThreadPool(threads);
        try {
            var startGate = new CountDownLatch(1);
            var futures = new ArrayList<Future<?>>();
            for (int t = 0; t < threads; t++) {
                int threadIdx = t;
                futures.add(executor.submit(() -> {
                    awaitGate(startGate);
                    int base = threadIdx * operationsPerThread;
                    for (int i = 0; i < operationsPerThread; i++) {
                        lifecycle.registerConnection(pcsms[base + i]);
                    }
                    // Deregister even-indexed half, leaving odd-indexed ones registered
                    for (int i = 0; i < operationsPerThread; i += 2) {
                        lifecycle.deregisterConnection(pcsms[base + i]);
                    }
                    return null;
                }));
            }
            startGate.countDown();
            for (Future<?> f : futures) {
                try {
                    f.get(30, TimeUnit.SECONDS);
                }
                catch (ExecutionException e) {
                    throw new AssertionError("worker thread failed", e.getCause());
                }
            }
        }
        finally {
            executor.shutdownNow();
        }

        Set<ProxyChannelStateMachine> expected = new HashSet<>();
        for (int t = 0; t < threads; t++) {
            int base = t * operationsPerThread;
            for (int i = 1; i < operationsPerThread; i += 2) {
                expected.add(pcsms[base + i]);
            }
        }
        assertThat(lifecycle.activeConnections()).containsExactlyInAnyOrderElementsOf(expected);
    }

    private static void awaitGate(CountDownLatch gate) {
        try {
            gate.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("test thread interrupted", e);
        }
    }
}
