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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DrainCoordinatorTest {

    private static final String CLUSTER = "cluster-A";

    @Test
    void registerAddsPcsmToCluster() {
        var coordinator = new DrainCoordinator();
        var pcsm = mock(ProxyChannelStateMachine.class);
        coordinator.register(CLUSTER, pcsm);
        assertThat(coordinator.activeConnectionsFor(CLUSTER)).containsExactly(pcsm);
    }

    @Test
    void deregisterRemovesPcsmAndDropsEmptyClusterEntry() {
        var coordinator = new DrainCoordinator();
        var pcsm = mock(ProxyChannelStateMachine.class);
        coordinator.register(CLUSTER, pcsm);
        coordinator.deregister(CLUSTER, pcsm);
        assertThat(coordinator.activeConnectionsFor(CLUSTER)).isEmpty();
    }

    @Test
    void deregisterKeepsClusterEntryWhenOtherPcsmsRemain() {
        var coordinator = new DrainCoordinator();
        var pcsm1 = mock(ProxyChannelStateMachine.class);
        var pcsm2 = mock(ProxyChannelStateMachine.class);
        coordinator.register(CLUSTER, pcsm1);
        coordinator.register(CLUSTER, pcsm2);
        coordinator.deregister(CLUSTER, pcsm1);
        assertThat(coordinator.activeConnectionsFor(CLUSTER)).containsExactly(pcsm2);
    }

    @Test
    void deregisterOfUnknownClusterIsNoOp() {
        var coordinator = new DrainCoordinator();
        var pcsm = mock(ProxyChannelStateMachine.class);
        coordinator.deregister("never-registered", pcsm);
        assertThat(coordinator.activeConnectionsFor("never-registered")).isEmpty();
    }

    /**
     * Targets the specific race the previous get()/remove() pair was vulnerable to:
     * Thread A is deregistering the last PCSM for a cluster while Thread B is registering
     * a new PCSM on the same cluster. The pre-fix code could lose Thread B's PCSM entirely
     * if the timing aligned (B's add slipped between A's isEmpty() check and A's
     * activeConnections.remove()).
     * <p>
     * Repeated 200 times to give the JVM scheduler many chances to expose the race.
     * With the compute()/computeIfPresent() fix in place this is deterministic.
     */
    @Test
    void concurrentRegisterAndDeregisterOnLastConnectionDoesNotLosePcsm() throws Exception {
        for (int iter = 0; iter < 200; iter++) {
            var coordinator = new DrainCoordinator();
            var pcsm1 = mock(ProxyChannelStateMachine.class);
            var pcsm2 = mock(ProxyChannelStateMachine.class);
            coordinator.register(CLUSTER, pcsm1);

            var startGate = new CountDownLatch(1);
            var deregisterer = new Thread(() -> {
                awaitGate(startGate);
                coordinator.deregister(CLUSTER, pcsm1);
            });
            var registerer = new Thread(() -> {
                awaitGate(startGate);
                coordinator.register(CLUSTER, pcsm2);
            });
            deregisterer.start();
            registerer.start();
            startGate.countDown();
            deregisterer.join();
            registerer.join();

            assertThat(coordinator.activeConnectionsFor(CLUSTER))
                    .as("iteration %d: pcsm2 must still be registered after concurrent register/deregister", iter)
                    .containsExactly(pcsm2);
        }
    }

    /**
     * Higher-volume contention test. Each thread registers a batch of distinct PCSMs and
     * deregisters half of them. The expected end state is exactly the never-deregistered
     * PCSMs from every thread — no PCSM lost, no ghost PCSM remaining. Catches lost
     * registrations, double-removals, and any future race introduced by careless changes
     * to register/deregister.
     */
    @Test
    void registerAndDeregisterAreThreadSafeUnderContention() throws Exception {
        int threads = 8;
        int operationsPerThread = 500;
        var coordinator = new DrainCoordinator();
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
                    // Register all assigned PCSMs.
                    for (int i = 0; i < operationsPerThread; i++) {
                        coordinator.register(CLUSTER, pcsms[base + i]);
                    }
                    // Deregister the even-indexed half, leaving the odd-indexed ones registered.
                    for (int i = 0; i < operationsPerThread; i += 2) {
                        coordinator.deregister(CLUSTER, pcsms[base + i]);
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
        assertThat(coordinator.activeConnectionsFor(CLUSTER))
                .containsExactlyInAnyOrderElementsOf(expected);
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

    @Test
    void drainClusterWithNoRegisteredConnectionsReturnsCompletedFuture() {
        // Given — no PCSMs registered for this cluster
        var coordinator = new DrainCoordinator();

        // When
        CompletableFuture<Void> result = coordinator.drainCluster(CLUSTER, Duration.ofSeconds(10));

        // Then — the no-active-connections fast path returns an already-complete future
        assertThat(result).isCompleted();
        assertThat(result).isNotCompletedExceptionally();
    }

    @Test
    void drainClusterInvokesInitiateCloseOnEachRegisteredPcsm() {
        // Given — three PCSMs registered for the cluster, each stubbed to return an
        // already-complete future so drainCluster's aggregate future resolves promptly
        var coordinator = new DrainCoordinator();
        var pcsm1 = mock(ProxyChannelStateMachine.class);
        var pcsm2 = mock(ProxyChannelStateMachine.class);
        var pcsm3 = mock(ProxyChannelStateMachine.class);
        when(pcsm1.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(pcsm2.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        when(pcsm3.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        coordinator.register(CLUSTER, pcsm1);
        coordinator.register(CLUSTER, pcsm2);
        coordinator.register(CLUSTER, pcsm3);

        // When
        var timeout = Duration.ofSeconds(7);
        CompletableFuture<Void> result = coordinator.drainCluster(CLUSTER, timeout);

        // Then — every registered PCSM had initiateClose invoked with the supplied timeout,
        // and the aggregate future is complete because all per-PCSM futures were complete
        verify(pcsm1).initiateClose(timeout);
        verify(pcsm2).initiateClose(timeout);
        verify(pcsm3).initiateClose(timeout);
        assertThat(result).isCompleted();
    }

    @Test
    void drainClusterFutureCompletesOnlyWhenEveryPcsmFutureCompletes() {
        // Given — two PCSMs whose initiateClose returns futures we control
        var coordinator = new DrainCoordinator();
        var pcsm1 = mock(ProxyChannelStateMachine.class);
        var pcsm2 = mock(ProxyChannelStateMachine.class);
        var f1 = new CompletableFuture<Void>();
        var f2 = new CompletableFuture<Void>();
        when(pcsm1.initiateClose(any())).thenReturn(f1);
        when(pcsm2.initiateClose(any())).thenReturn(f2);
        coordinator.register(CLUSTER, pcsm1);
        coordinator.register(CLUSTER, pcsm2);

        // When
        CompletableFuture<Void> aggregate = coordinator.drainCluster(CLUSTER, Duration.ofSeconds(10));

        // Then — aggregate is incomplete while either per-PCSM future is pending
        assertThat(aggregate).isNotCompleted();

        // When the first completes, the aggregate is still incomplete
        f1.complete(null);
        assertThat(aggregate).isNotCompleted();

        // Only when both complete does the aggregate complete
        f2.complete(null);
        assertThat(aggregate).isCompleted();
    }

    @Test
    void drainClusterIgnoresPcsmsRegisteredForOtherClusters() {
        // Given — a PCSM registered for a different cluster than the one being drained
        var coordinator = new DrainCoordinator();
        var pcsmInOtherCluster = mock(ProxyChannelStateMachine.class);
        coordinator.register("some-other-cluster", pcsmInOtherCluster);

        // When — drain the (empty) target cluster
        CompletableFuture<Void> result = coordinator.drainCluster(CLUSTER, Duration.ofSeconds(10));

        // Then — drain completes immediately and the unrelated PCSM is never touched
        assertThat(result).isCompleted();
        verify(pcsmInOtherCluster, never()).initiateClose(any());
    }
}
