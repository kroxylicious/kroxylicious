/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import org.assertj.core.api.Assumptions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class VirtualClusterCoordinatorTest {

    private static final String CLUSTER_A = "cluster-a";
    private static final String CLUSTER_B = "cluster-b";

    @SuppressWarnings("unchecked")
    private final BiConsumer<String, Optional<Throwable>> noOpCallback = mock(BiConsumer.class);

    private VirtualClusterCoordinator vcc;

    private static VirtualClusterModel mockModel(String name) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        when(model.drainTimeout()).thenReturn(Duration.ofSeconds(30));
        return model;
    }

    private VirtualClusterLifecycle requireLifecycle(String name) {
        var lifecycle = vcc.lifecycleFor(name);
        Assumptions.assumeThat(lifecycle).as("lifecycle for '%s' should exist", name).isNotNull();
        return lifecycle;
    }

    @BeforeEach
    void setUp() {
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A)), noOpCallback);
    }

    @Test
    void shouldCreateLifecycleManagerInInitializingState() {
        // when
        var manager = vcc.lifecycleFor(CLUSTER_A);

        // then
        assertThat(manager).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Initializing.class);
    }

    @Test
    void shouldCreateLifecycleManagerForEachModel() {
        // given
        var multiVcm = new VirtualClusterCoordinator(
                List.of(mockModel("cluster-a"), mockModel(CLUSTER_B)),
                noOpCallback);

        // when/then
        assertThat(multiVcm.lifecycleFor("cluster-a")).isNotNull();
        assertThat(multiVcm.lifecycleFor(CLUSTER_B)).isNotNull();
    }

    @Test
    void shouldReturnNullForUnknownCluster() {
        // when
        var result = vcc.lifecycleFor("nonexistent");

        // then
        assertThat(result).isNull();
    }

    @Test
    void shouldExposeVirtualClusterModels() {
        // given
        var modelA = mockModel("cluster-a");
        var modelB = mockModel(CLUSTER_B);
        var multiVcm = new VirtualClusterCoordinator(List.of(modelA, modelB), noOpCallback);

        // when
        var models = multiVcm.virtualClusterModels();

        // then
        assertThat(models).containsExactly(modelA, modelB);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullModels() {
        assertThatThrownBy(() -> new VirtualClusterCoordinator(null, noOpCallback))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullCallback() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterCoordinator(virtualClusterModels, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectDuplicateClusterNames() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterCoordinator(
                virtualClusterModels,
                noOpCallback))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CLUSTER_A);
    }

    // Per-VC lifecycle transitions

    @Test
    void shouldTransitionNamedClusterToServing() {
        // when
        vcc.initializationSucceeded(CLUSTER_A);

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Serving.class);
    }

    @Test
    void shouldNotFireCallbackOnInitializationSuccess() {
        // when
        vcc.initializationSucceeded(CLUSTER_A);

        // then
        verifyNoInteractions(noOpCallback);
    }

    @Test
    void shouldTransitionToStoppedOnInitializationFailure() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcc.initializationFailed(CLUSTER_A, cause);

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackOnInitializationFailure() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcc.initializationFailed(CLUSTER_A, cause);

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.of(cause));
    }

    @Test
    void shouldRetainFailureCauseInStoppedState() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcc.initializationFailed(CLUSTER_A, cause);

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOfSatisfying(VirtualClusterLifecycleState.Stopped.class,
                        stopped -> assertThat(stopped.priorFailureCause()).isSameAs(cause));
    }

    @Test
    void shouldThrowForUnknownClusterOnInitializationSucceeded() {
        // when/then
        assertThatThrownBy(() -> vcc.initializationSucceeded("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowForUnknownClusterOnInitializationFailed() {
        // when/then
        RuntimeException boom = new RuntimeException("boom");
        assertThatThrownBy(() -> vcc.initializationFailed("nonexistent", boom))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Bulk shutdown transitions

    @Test
    void shouldTransitionServingToStoppedOnShutdownWhenNoConnections() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);

        // when
        vcc.initiateShutdown();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldTransitionInitializingToStoppedOnBulkDrain() {
        // when
        vcc.initiateShutdown();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackForInitializingStoppedDuringBulkDrain() {
        // when
        vcc.initiateShutdown();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldFireCallbackForServingClusterOnShutdownWhenNoConnections() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);

        // when
        vcc.initiateShutdown();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldDrainAllClustersInParallel() throws InterruptedException {
        // Given — two clusters, each with one connection.
        // clusterA's drain blocks indefinitely; clusterB's drain completes immediately.
        // If drains are initiated in parallel, B is called while A is still pending.
        // If initiated sequentially (await A before starting B), B is never called and
        // Awaitility times out with a clear assertion failure.
        var pendingDrainA = new CompletableFuture<Void>();

        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.initializationSucceeded(CLUSTER_B);

        var pcsmA = mock(ProxyChannelStateMachine.class);
        when(pcsmA.initiateClose(any())).thenReturn(pendingDrainA);

        var pcsmB = mock(ProxyChannelStateMachine.class);
        when(pcsmB.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));

        vcc.registerConnection(CLUSTER_A, pcsmA);
        vcc.registerConnection(CLUSTER_B, pcsmB);

        // Run shutdown on a background thread since it blocks until all drains complete
        var shutdownThread = new Thread(() -> vcc.initiateShutdown());
        shutdownThread.start();

        // then — B must be called even while A is still draining
        Awaitility.await("cluster-b drain should start while cluster-a is still draining")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(pcsmB).initiateClose(any()));

        // cleanup
        pendingDrainA.complete(null);
        shutdownThread.join(5000);
    }

    @Test
    void shouldKeepServingClusterInDrainingWhileConnectionIsPending() {
        // given
        var pendingDrain = new CompletableFuture<Void>();
        var pcsm = mock(ProxyChannelStateMachine.class);
        when(pcsm.initiateClose(any())).thenReturn(pendingDrain);
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, pcsm);

        // Run shutdown on a separate thread since it blocks until drain completes
        var shutdownThread = new Thread(() -> vcc.initiateShutdown());
        shutdownThread.start();

        // then — cluster stays Draining while the connection is pending
        assertThat(shutdownThread.isAlive()).isTrue();
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Draining.class);

        // cleanup
        pendingDrain.complete(null);
    }

    @Test
    void shouldTransitionDrainingToStoppedOnBulkStop() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.initiateShutdown();

        // when
        vcc.completeDraining();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackWithEmptyCauseOnBulkStop() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.initiateShutdown();

        // when
        vcc.completeDraining();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldReturnTrueWhenAllClustersStopped() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.initiateShutdown();

        // when
        var allStopped = vcc.completeDraining();

        // then
        assertThat(allStopped).isTrue();
    }

    @Test
    void shouldReturnFalseWhenNotAllClustersStopped() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);

        // when
        var allStopped = vcc.completeDraining();

        // then
        assertThat(allStopped).isFalse();
    }

    @Test
    void shouldStopServingClustersWhenShuttingDownWithNoConnections() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldStopInitializingClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_B))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldStopFailedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // Reach Failed state directly on the lifecycle, bypassing VirtualClusterCoordinator.initializationFailed()
        // which currently auto-transitions Failed → Stopped. Once retry/rollback is implemented, a cluster
        // will be able to sit in Failed without being immediately stopped, making this state reachable via
        // the coordinator's normal API.
        var badThingsHappenedHere = new IllegalStateException("bad things happened here");
        requireLifecycle(CLUSTER_B).initializationFailed(badThingsHappenedHere);

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_B))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .asInstanceOf(InstanceOfAssertFactories.type(VirtualClusterLifecycleState.Stopped.class))
                .satisfies(state -> assertThat(state.priorFailureCause()).isEqualTo(badThingsHappenedHere));
        verify(noOpCallback).accept(CLUSTER_B, Optional.of(badThingsHappenedHere));
    }

    @Test
    void shouldLeaveDrainingClustersInDraining() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // Force into Draining directly, bypassing initiateShutdown(), to simulate
        // a cluster mid-drain (e.g. hot-reload) when shutdown is called.
        requireLifecycle(CLUSTER_A).startDraining();

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Draining.class);
    }

    @Test
    void shouldNotFireCallbackForDrainingClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A)), noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);
        requireLifecycle(CLUSTER_A).startDraining();

        // When
        vcc.initiateShutdown();

        // Then
        verifyNoInteractions(noOpCallback);
    }

    @Test
    void shouldLeaveAlreadyStoppedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A)), noOpCallback);

        // Force into Stopped directly, bypassing the coordinator's auto-stop logic.
        requireLifecycle(CLUSTER_A).stop();

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldNotFireCallbackForAlreadyStoppedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A)), noOpCallback);
        requireLifecycle(CLUSTER_A).stop();

        // When
        vcc.initiateShutdown();

        // Then
        verifyNoInteractions(noOpCallback);
    }

    @Test
    void shouldHandleAllStatesCorrectlyDuringShutdown() {
        // Given — one cluster in each possible lifecycle state
        var serving = "serving";
        var initializing = "initializing";
        var draining = "draining";
        var failed = "failed";
        var stopped = "stopped";
        var failureCause = new RuntimeException("init failed");

        vcc = new VirtualClusterCoordinator(
                List.of(mockModel(serving), mockModel(initializing), mockModel(draining), mockModel(failed), mockModel(stopped)),
                noOpCallback);

        vcc.initializationSucceeded(serving);

        // Force draining, failed, and stopped states directly — bypassing the coordinator's
        // auto-stop logic to simulate states that will be reachable via the normal API
        // once hot-reload and retry/rollback are implemented.
        requireLifecycle(draining).initializationSucceeded();
        requireLifecycle(draining).startDraining();
        requireLifecycle(failed).initializationFailed(failureCause);
        requireLifecycle(stopped).initializationFailed(failureCause);
        requireLifecycle(stopped).stop();

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(requireLifecycle(serving).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(initializing).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(draining).state()).isInstanceOf(VirtualClusterLifecycleState.Draining.class);
        assertThat(requireLifecycle(failed).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(stopped).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        verify(noOpCallback).accept(serving, Optional.empty());
        verify(noOpCallback).accept(initializing, Optional.empty());
        verify(noOpCallback).accept(failed, Optional.of(failureCause));
    }

    @Test
    void shouldStopInitialisingWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterCoordinator(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), noOpCallback);

        // When
        vcc.initiateShutdown();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(vcc.lifecycleFor(CLUSTER_B))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    // Connection registration

    @Test
    void shouldTrackRegisteredConnectionForCluster() {
        // given
        var pcsm = mock(ProxyChannelStateMachine.class);

        // when
        vcc.registerConnection(CLUSTER_A, pcsm);

        // then
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEqualTo(Set.of(pcsm));
    }

    @Test
    void shouldRemoveConnectionOnDeregister() {
        // given
        var pcsm = mock(ProxyChannelStateMachine.class);
        vcc.registerConnection(CLUSTER_A, pcsm);

        // when
        vcc.deregisterConnection(CLUSTER_A, pcsm);

        // then
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldReturnEmptySetForClusterWithNoConnections() {
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldThrowForUnknownClusterOnRegisterConnection() {
        assertThatThrownBy(() -> vcc.registerConnection("nonexistent", mock(ProxyChannelStateMachine.class)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowForUnknownClusterOnDeregisterConnection() {
        assertThatThrownBy(() -> vcc.deregisterConnection("nonexistent", mock(ProxyChannelStateMachine.class)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowForUnknownClusterOnActiveConnectionsFor() {
        assertThatThrownBy(() -> vcc.activeConnectionsFor("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Drain on shutdown

    @Test
    void shouldInitiateCloseOnRegisteredConnectionWhenShuttingDown() {
        // given
        var pcsm = mock(ProxyChannelStateMachine.class);
        when(pcsm.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, pcsm);

        // when
        vcc.initiateShutdown();

        // then
        verify(pcsm).initiateClose(Duration.ofSeconds(30));
    }

    @Test
    void shouldTransitionToStoppedAfterConnectionsDrainOnShutdown() {
        // given
        var pcsm = mock(ProxyChannelStateMachine.class);
        when(pcsm.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, pcsm);

        // when
        vcc.initiateShutdown();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireStoppedCallbackAfterConnectionsDrainOnShutdown() {
        // given
        var pcsm = mock(ProxyChannelStateMachine.class);
        when(pcsm.initiateClose(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, pcsm);

        // when
        vcc.initiateShutdown();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }
}
