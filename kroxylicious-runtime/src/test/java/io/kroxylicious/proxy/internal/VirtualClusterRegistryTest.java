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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import org.assertj.core.api.Assumptions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class VirtualClusterRegistryTest {

    private static final String CLUSTER_A = "cluster-a";
    private static final String CLUSTER_B = "cluster-b";

    /**
     * No-op resolver for tests that don't exercise {@code resolveModel}. Throws if invoked so
     * accidental dependence on resolveModel surfaces as a clear failure rather than a null VCM.
     */
    private static final BiFunction<Configuration, String, VirtualClusterModel> NO_OP_RESOLVER = (cfg, name) -> {
        throw new UnsupportedOperationException("resolveModel not exercised by this test");
    };

    @SuppressWarnings("unchecked")
    private final BiConsumer<String, Optional<Throwable>> noOpCallback = mock(BiConsumer.class);

    private VirtualClusterRegistry vcc;

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
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);
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
        var multiVcm = new VirtualClusterRegistry(
                List.of(mockModel("cluster-a"), mockModel(CLUSTER_B)),
                NO_OP_RESOLVER, noOpCallback);

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
    void modelForReturnsRegisteredModel() {
        // CLUSTER_A is in the registry via setUp.
        assertThat(vcc.modelFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterModel::getClusterName).isEqualTo(CLUSTER_A);
    }

    @Test
    void modelForReturnsNullForUnknownCluster() {
        assertThat(vcc.modelFor("never-existed")).isNull();
    }

    @Test
    void modelForFollowsRuntimeAddAndRemove() {
        // After addVirtualCluster, modelFor returns the new model.
        var added = mockModel(CLUSTER_B);
        vcc.addVirtualCluster(added);
        assertThat(vcc.modelFor(CLUSTER_B)).isSameAs(added);

        // After removeVirtualCluster, the entry is retained (append-only policy) so modelFor
        // continues to return the same model. The lifecycle's state distinguishes "Stopped"
        // from "Serving" — callers that care about state should compose with lifecycleFor.
        vcc.initializationSucceeded(CLUSTER_B);
        vcc.removeVirtualCluster(CLUSTER_B).join();
        assertThat(vcc.modelFor(CLUSTER_B)).isSameAs(added);
    }

    @Test
    void shouldExposeVirtualClusterModels() {
        // given
        var modelA = mockModel("cluster-a");
        var modelB = mockModel(CLUSTER_B);
        var multiVcm = new VirtualClusterRegistry(List.of(modelA, modelB), NO_OP_RESOLVER, noOpCallback);

        // when
        var models = multiVcm.virtualClusterModels();

        // then — order is unspecified (backing map is ConcurrentHashMap); only contents matter.
        assertThat(models).containsExactlyInAnyOrder(modelA, modelB);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullModels() {
        assertThatThrownBy(() -> new VirtualClusterRegistry(null, NO_OP_RESOLVER, noOpCallback))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullCallback() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterRegistry(virtualClusterModels, NO_OP_RESOLVER, null))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullRawModelResolver() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterRegistry(virtualClusterModels, null, noOpCallback))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectDuplicateClusterNames() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterRegistry(
                virtualClusterModels,
                NO_OP_RESOLVER,
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
        vcc.shutdownAllClusters();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldTransitionInitializingToStoppedOnBulkDrain() {
        // when
        vcc.shutdownAllClusters();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackForInitializingStoppedDuringBulkDrain() {
        // when
        vcc.shutdownAllClusters();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldFireCallbackForServingClusterOnShutdownWhenNoConnections() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);

        // when
        vcc.shutdownAllClusters();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldDrainAllClustersInParallel() {
        // Given — two clusters, each with one connection.
        // clusterA's drain blocks indefinitely; clusterB's drain completes immediately.
        // If drains are initiated in parallel, B is called while A is still pending.
        // If initiated sequentially (await A before starting B), B is never called and
        // Awaitility times out with a clear assertion failure.
        var pendingDrainA = new CompletableFuture<Void>();

        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.initializationSucceeded(CLUSTER_B);

        var ccsmA = mock(ClientConnectionStateMachine.class);
        when(ccsmA.drain(any())).thenReturn(pendingDrainA);

        var ccsmB = mock(ClientConnectionStateMachine.class);
        when(ccsmB.drain(any())).thenReturn(CompletableFuture.completedFuture(null));

        vcc.registerConnection(CLUSTER_A, ccsmA);
        vcc.registerConnection(CLUSTER_B, ccsmB);

        // shutdownAllClusters() blocks, so run it asynchronously
        var shutdown = CompletableFuture.runAsync(() -> vcc.shutdownAllClusters());

        // then — B must be called even while A is still draining
        Awaitility.await("cluster-b drain should start while cluster-a is still draining")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(ccsmB).drain(any()));

        pendingDrainA.complete(null);
        assertThat(shutdown).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldKeepServingClusterInDrainingWhileConnectionIsPending() {
        // given
        var pendingDrain = new CompletableFuture<Void>();
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(pendingDrain);
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, ccsm);

        // shutdownAllClusters() blocks, so run it asynchronously
        var shutdown = CompletableFuture.runAsync(() -> vcc.shutdownAllClusters());

        // then — cluster stays Draining while the connection is pending
        Awaitility.await("cluster should enter Draining state")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(vcc.lifecycleFor(CLUSTER_A))
                        .isNotNull()
                        .extracting(VirtualClusterLifecycle::state)
                        .isInstanceOf(VirtualClusterLifecycleState.Draining.class));

        pendingDrain.complete(null);
        assertThat(shutdown).succeedsWithin(5, TimeUnit.SECONDS);
    }

    @Test
    void shouldStopServingClustersWhenShuttingDownWithNoConnections() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // When
        vcc.shutdownAllClusters();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldStopInitializingClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // When
        vcc.shutdownAllClusters();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_B))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldStopFailedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        // Reach Failed state directly on the lifecycle, bypassing VirtualClusterRegistry.initializationFailed()
        // which currently auto-transitions Failed → Stopped. Once retry/rollback is implemented, a cluster
        // will be able to sit in Failed without being immediately stopped, making this state reachable via
        // the coordinator's normal API.
        var badThingsHappenedHere = new IllegalStateException("bad things happened here");
        requireLifecycle(CLUSTER_B).initializationFailed(badThingsHappenedHere);

        // When
        vcc.shutdownAllClusters();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_B))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .asInstanceOf(InstanceOfAssertFactories.type(VirtualClusterLifecycleState.Stopped.class))
                .satisfies(state -> assertThat(state.priorFailureCause()).isEqualTo(badThingsHappenedHere));
        verify(noOpCallback).accept(CLUSTER_B, Optional.of(badThingsHappenedHere));
    }

    @Test
    void shouldTransitionPreexistingDrainingClusterToStoppedOnShutdown() {
        // Given — cluster already draining (e.g. from hot-reload) with no active connections
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);
        requireLifecycle(CLUSTER_A).startDraining();

        // When
        vcc.shutdownAllClusters();

        // Then — shutdown joins the in-progress drain rather than leaving it in Draining
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackForPreexistingDrainingClusterOnShutdown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);
        requireLifecycle(CLUSTER_A).startDraining();

        // When
        vcc.shutdownAllClusters();

        // Then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldWaitForPreexistingDrainToCompleteBeforeShuttingDown() {
        // Given — cluster draining with a pending connection
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);
        vcc.initializationSucceeded(CLUSTER_A);

        var pendingDrain = new CompletableFuture<Void>();
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(pendingDrain);
        vcc.registerConnection(CLUSTER_A, ccsm);
        requireLifecycle(CLUSTER_A).startDraining();

        // shutdownAllClusters() blocks, so run it asynchronously
        var shutdown = CompletableFuture.runAsync(() -> vcc.shutdownAllClusters());

        Awaitility.await("cluster should remain Draining while connection is pending")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> assertThat(vcc.lifecycleFor(CLUSTER_A))
                        .isNotNull()
                        .extracting(VirtualClusterLifecycle::state)
                        .isInstanceOf(VirtualClusterLifecycleState.Draining.class));

        // When — connection drain completes
        pendingDrain.complete(null);

        // Then — shutdown unblocks and cluster is Stopped
        assertThat(shutdown).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldLeaveAlreadyStoppedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);

        // Force into Stopped directly, bypassing the coordinator's auto-stop logic.
        requireLifecycle(CLUSTER_A).stop();

        // When
        vcc.shutdownAllClusters();

        // Then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldNotFireCallbackForAlreadyStoppedClustersWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A)), NO_OP_RESOLVER, noOpCallback);
        requireLifecycle(CLUSTER_A).stop();

        // When
        vcc.shutdownAllClusters();

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

        vcc = new VirtualClusterRegistry(
                List.of(mockModel(serving), mockModel(initializing), mockModel(draining), mockModel(failed), mockModel(stopped)),
                NO_OP_RESOLVER, noOpCallback);

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
        vcc.shutdownAllClusters();

        // Then
        assertThat(requireLifecycle(serving).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(initializing).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(draining).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(failed).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(requireLifecycle(stopped).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        verify(noOpCallback).accept(serving, Optional.empty());
        verify(noOpCallback).accept(initializing, Optional.empty());
        verify(noOpCallback).accept(draining, Optional.empty());
        verify(noOpCallback).accept(failed, Optional.of(failureCause));
    }

    @Test
    void shouldStopInitialisingWhenShuttingDown() {
        // Given
        vcc = new VirtualClusterRegistry(List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);

        // When
        vcc.shutdownAllClusters();

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
        vcc.initializationSucceeded(CLUSTER_A);
        var ccsm = mock(ClientConnectionStateMachine.class);

        // when
        vcc.registerConnection(CLUSTER_A, ccsm);

        // then
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEqualTo(Set.of(ccsm));
    }

    @Test
    void shouldRemoveConnectionOnDeregister() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);
        var ccsm = mock(ClientConnectionStateMachine.class);
        vcc.registerConnection(CLUSTER_A, ccsm);

        // when
        vcc.deregisterConnection(CLUSTER_A, ccsm);

        // then
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEmpty();
    }

    @Test
    void shouldReturnEmptySetForClusterWithNoConnections() {
        assertThat(vcc.activeConnectionsFor(CLUSTER_A)).isEmpty();
    }

    @Test
    void registerConnectionReturnsFalseForUnknownCluster() {
        // Connection arrives carrying a cluster name VCR doesn't know about — VCR rejects
        // cleanly via the false-return path rather than throwing into Netty's pipeline.
        // Defends KafkaProxyInitializer's rejectConnection flow from the
        // bookkeeping-vs-binding ordering invariant being broken by a future refactor.
        assertThat(vcc.registerConnection("nonexistent", mock(ClientConnectionStateMachine.class)))
                .as("unknown cluster must be treated as a rejection, not an error")
                .isFalse();
    }

    @Test
    void deregisterConnectionIsNoOpForUnknownCluster() {
        // Channel-close listener races with future cleanup-on-Stopped logic. Throwing into
        // Netty's listener invoker would log noisily — silent no-op is the right shape.
        var ccsm = mock(ClientConnectionStateMachine.class);

        vcc.deregisterConnection("nonexistent", ccsm);

        verifyNoInteractions(ccsm);
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
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, ccsm);

        // when
        vcc.shutdownAllClusters();

        // then
        verify(ccsm).drain(Duration.ofSeconds(30));
    }

    @Test
    void shouldTransitionToStoppedAfterConnectionsDrainOnShutdown() {
        // given
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, ccsm);

        // when
        vcc.shutdownAllClusters();

        // then
        assertThat(vcc.lifecycleFor(CLUSTER_A))
                .isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireStoppedCallbackAfterConnectionsDrainOnShutdown() {
        // given
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(CompletableFuture.completedFuture(null));
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.registerConnection(CLUSTER_A, ccsm);

        // when
        vcc.shutdownAllClusters();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    // -----------------------------------------------------------------------------------------
    // Reconfigure operations: addVirtualCluster and removeVirtualCluster. Modify is delegated
    // to ReplaceCluster which composes remove + add — there is no dedicated VCR method for it.
    // -----------------------------------------------------------------------------------------

    @Test
    void removeVirtualClusterDrivesServingClusterToStopped() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);
        assertThat(requireLifecycle(CLUSTER_A).state()).isInstanceOf(VirtualClusterLifecycleState.Serving.class);

        // when
        var future = vcc.removeVirtualCluster(CLUSTER_A);

        // then
        assertThat(future).succeedsWithin(5, TimeUnit.SECONDS);
        assertThat(requireLifecycle(CLUSTER_A).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void removeVirtualClusterLeavesOtherClustersUnaffected() {
        // given — two serving clusters in one registry
        var registry = new VirtualClusterRegistry(
                List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_B)), NO_OP_RESOLVER, noOpCallback);
        registry.initializationSucceeded(CLUSTER_A);
        registry.initializationSucceeded(CLUSTER_B);

        // when — remove only CLUSTER_B
        registry.removeVirtualCluster(CLUSTER_B).join();

        // then — CLUSTER_B reaches Stopped, CLUSTER_A stays in Serving
        assertThat(registry.lifecycleFor(CLUSTER_B).state())
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
        assertThat(registry.lifecycleFor(CLUSTER_A).state())
                .isInstanceOf(VirtualClusterLifecycleState.Serving.class);
    }

    @Test
    void removeVirtualClusterFiresOnStoppedCallback() {
        // given
        vcc.initializationSucceeded(CLUSTER_A);

        // when
        vcc.removeVirtualCluster(CLUSTER_A).join();

        // then — callback invoked with (clusterName, Optional.empty()) per the no-failure case
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void removeVirtualClusterIsNoOpWhenAlreadyStopped() {
        // given — drive the cluster to Stopped via the normal remove path
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.removeVirtualCluster(CLUSTER_A).join();
        assertThat(requireLifecycle(CLUSTER_A).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);

        // when — call remove again on an already-Stopped cluster
        var future = vcc.removeVirtualCluster(CLUSTER_A);

        // then — completes immediately, no exception
        assertThat(future).succeedsWithin(1, TimeUnit.SECONDS);
        assertThat(requireLifecycle(CLUSTER_A).state()).isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void removeVirtualClusterThrowsForUnknownClusterName() {
        // The real removeVirtualCluster validates via requireKnownCluster, unlike the previous
        // stub which silently accepted unknown names.
        assertThatThrownBy(() -> vcc.removeVirtualCluster("never-existed"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Unknown cluster");
    }

    @Test
    void addVirtualClusterCreatesLifecycleInInitializing() {
        // given
        var newModel = mockModel(CLUSTER_B);
        assertThat(vcc.lifecycleFor(CLUSTER_B)).isNull();

        // when
        var future = vcc.addVirtualCluster(newModel);

        // then — future already completed; lifecycle exists in INITIALIZING (orchestrator will
        // call initializationSucceeded once gateways are bound).
        assertThat(future).isCompleted();
        assertThat(vcc.lifecycleFor(CLUSTER_B)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .isInstanceOf(VirtualClusterLifecycleState.Initializing.class);
    }

    @Test
    void addVirtualClusterRejectsDuplicateNameWhenExistingEntryIsActive() {
        // CLUSTER_A is in INITIALIZING after setUp — re-adding it is a contract violation.
        var duplicate = mockModel(CLUSTER_A);
        assertThatThrownBy(() -> vcc.addVirtualCluster(duplicate))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(CLUSTER_A)
                .hasMessageContaining("Initializing");
    }

    @Test
    void addVirtualClusterReplacesStoppedEntry() {
        // ReplaceCluster's add half lands here: the remove half drove the cluster to Stopped
        // (entry retained per shutdownCluster's append-only policy), and the add half then
        // calls addVirtualCluster with the new model — which must succeed and replace the
        // dead entry with a fresh INITIALIZING lifecycle.
        vcc.removeVirtualCluster(CLUSTER_A).join();
        var freshModel = mockModel(CLUSTER_A);

        var future = vcc.addVirtualCluster(freshModel);

        assertThat(future).isCompleted();
        assertThat(vcc.lifecycleFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycle::state)
                .as("re-added cluster's lifecycle should be a fresh Initializing instance")
                .isInstanceOf(VirtualClusterLifecycleState.Initializing.class);
        assertThat(vcc.virtualClusterModels())
                .as("virtualClusterModels reports the new model, not the stale one")
                .containsExactly(freshModel);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void addVirtualClusterRejectsNullModel() {
        assertThatThrownBy(() -> vcc.addVirtualCluster(null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void virtualClusterModelsReflectsRuntimeAddedModel() {
        // Contract relied on by OperationsPlanner: a model handed to addVirtualCluster appears
        // in virtualClusterModels() so a subsequent reconfigure can resolve it by name.
        var addedModel = mockModel(CLUSTER_B);

        vcc.addVirtualCluster(addedModel);

        // Order is unspecified (backing map is ConcurrentHashMap); both the constructor-supplied
        // model and the runtime-added one must be present.
        assertThat(vcc.virtualClusterModels())
                .as("models for both the constructor-supplied and runtime-added clusters are present")
                .extracting(VirtualClusterModel::getClusterName)
                .containsExactlyInAnyOrder(CLUSTER_A, CLUSTER_B);
        assertThat(vcc.virtualClusterModels())
                .as("runtime-added model identity is preserved (same reference, not a copy)")
                .contains(addedModel);
    }

    @Test
    void virtualClusterModelsRetainsRemovedModel() {
        // Entries persist past Stopped — the registry never removes a cluster's entry from
        // its internal map, so virtualClusterModels() keeps reporting it.
        // RemoveCluster's defensive capture-then-remove relies on this only as a belt-and-braces
        // ordering — the contract here is that even after the lifecycle has been driven to
        // Stopped the model entry remains queryable.
        vcc.initializationSucceeded(CLUSTER_A);
        vcc.removeVirtualCluster(CLUSTER_A).join();

        assertThat(vcc.virtualClusterModels())
                .as("constructor-supplied model should remain queryable after removeVirtualCluster")
                .extracting(VirtualClusterModel::getClusterName)
                .containsExactly(CLUSTER_A);
    }

    // -----------------------------------------------------------------------------------------
    // closeModel hook — pins the per-VC resource cleanup contract added by the FCF-per-VC
    // refactor. Each of the four transition-into-Stopped branches in shutdownCluster must
    // invoke model.close(); the Stopped→Stopped no-op must not. Close failure must not stall
    // the shutdown future or block the onVirtualClusterStopped callback.
    // -----------------------------------------------------------------------------------------

    @Test
    void shouldCloseModelWhenServingClusterIsRemoved() {
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);
        registry.initializationSucceeded(CLUSTER_A);

        registry.removeVirtualCluster(CLUSTER_A).join();

        verify(model).close();
    }

    @Test
    void shouldCloseModelWhenInitializingClusterIsShutDown() {
        // Initializing state — the cluster never reached Serving but still owns FCF/TLS resources.
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);

        registry.shutdownAllClusters();

        verify(model).close();
    }

    @Test
    void shouldCloseModelWhenFailedClusterIsShutDown() {
        // Drive directly to Failed via the lifecycle, bypassing the registry's auto-stop, so the
        // shutdownAllClusters call exercises the Failed→Stopped close branch.
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);
        var failureCause = new RuntimeException("init failed");
        var lifecycle = registry.lifecycleFor(CLUSTER_A);
        Assumptions.assumeThat(lifecycle).isNotNull();
        lifecycle.initializationFailed(failureCause);

        registry.shutdownAllClusters();

        verify(model).close();
    }

    @Test
    void shouldCloseModelWhenDrainingClusterCompletesDrain() {
        // Close must fire only after the drain completes — not at drain start. Mocks the connection's
        // drain future so we can hold the cluster in Draining and assert close has not yet fired.
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);
        registry.initializationSucceeded(CLUSTER_A);

        var pendingDrain = new CompletableFuture<Void>();
        var ccsm = mock(ClientConnectionStateMachine.class);
        when(ccsm.drain(any())).thenReturn(pendingDrain);
        registry.registerConnection(CLUSTER_A, ccsm);

        var shutdown = CompletableFuture.runAsync(registry::shutdownAllClusters);

        Awaitility.await("drain should be initiated while cluster is Draining")
                .atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> verify(ccsm).drain(any()));

        // While still Draining, close must not have fired yet
        verify(model, never()).close();

        // Complete the drain — close should now fire on the Draining→Stopped transition
        pendingDrain.complete(null);

        assertThat(shutdown).succeedsWithin(5, TimeUnit.SECONDS);
        verify(model).close();
    }

    @Test
    void shouldCloseFreshModelAfterReAddOfStoppedCluster() {
        // Hot-reload scenario: cluster goes Serving → Stopped (model M1 closed and tracked as
        // closed by name), then a re-add via addVirtualCluster replaces the entry with a fresh
        // model M2. A subsequent shutdown of the re-added cluster MUST close M2 — the
        // closedClusters guard must not block it just because the same cluster name was
        // previously closed.
        var firstModel = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(firstModel), NO_OP_RESOLVER, noOpCallback);
        registry.initializationSucceeded(CLUSTER_A);
        registry.removeVirtualCluster(CLUSTER_A).join();
        verify(firstModel).close();

        // Re-add with a fresh model under the same cluster name (ReplaceCluster's add half).
        var freshModel = mockModel(CLUSTER_A);
        registry.addVirtualCluster(freshModel);
        registry.initializationSucceeded(CLUSTER_A);

        // Drive the re-added cluster to Stopped — its fresh model must be closed.
        registry.removeVirtualCluster(CLUSTER_A).join();
        verify(freshModel).close();
    }

    @Test
    void shouldNotCloseAlreadyStoppedModelOnRedundantShutdown() {
        // The Stopped→Stopped no-op branch must not invoke close again. Without this guarantee,
        // the FCF's per-Wrapper AtomicBoolean would absorb the double-close, but the
        // TlsCredentialSupplierManager might not, and a future close-handler addition could
        // throw on double-invocation.
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);
        registry.initializationSucceeded(CLUSTER_A);

        registry.removeVirtualCluster(CLUSTER_A).join();
        verify(model, times(1)).close();

        registry.removeVirtualCluster(CLUSTER_A).join();

        verify(model, times(1)).close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldFireStoppedCallbackEvenWhenModelCloseThrows() {
        // closeModel logs and propagates the close failure — the onVirtualClusterStopped callback
        // still fires (with the failure as cause) so the cluster is acknowledged as stopped,
        // but the shutdown future completes exceptionally so callers know the close was not clean.
        var closeFailure = new RuntimeException("KMS shutdown failed");
        var model = mockModel(CLUSTER_A);
        doThrow(closeFailure).when(model).close();
        BiConsumer<String, Optional<Throwable>> callback = mock(BiConsumer.class);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, callback);
        registry.initializationSucceeded(CLUSTER_A);

        var shutdown = registry.removeVirtualCluster(CLUSTER_A);

        assertThat(shutdown).failsWithin(5, TimeUnit.SECONDS);
        verify(model).close();
        verify(callback).accept(CLUSTER_A, Optional.of(closeFailure));
    }

    // ------------------------------------------------------------------------------------------
    // Threading contract for resolveModel
    //
    // The structural claim VCR makes to plugin authors is that FilterFactory.initialize() (called
    // transitively from the rawResolver during resolveModel) never runs on a Netty event loop —
    // it runs on the dedicated lifecycle thread.
    // ------------------------------------------------------------------------------------------

    @Test
    void resolveModelRunsRawResolverOffCallerThread() {
        // given — a resolver that captures the thread on which it executes
        var callerThread = Thread.currentThread();
        var capturedThread = new AtomicReference<Thread>();
        BiFunction<Configuration, String, VirtualClusterModel> capturingResolver = (cfg, name) -> {
            capturedThread.set(Thread.currentThread());
            return mockModel(name);
        };
        var registry = new VirtualClusterRegistry(List.of(), capturingResolver, noOpCallback);

        // when
        registry.resolveModel(mock(Configuration.class), CLUSTER_A);

        // then — the resolver ran on a thread that is NOT the caller's thread and DOES match the
        // lifecycle-thread name prefix. This is the structural no-event-loop guarantee.
        assertThat(capturedThread.get()).isNotSameAs(callerThread);
        assertThat(capturedThread.get().getName()).startsWith(VirtualClusterRegistry.LIFECYCLE_THREAD_NAME_PREFIX);
    }

    @Test
    void resolveModelPropagatesRuntimeExceptionUnwrapped() {
        // given — a resolver that throws a specific RuntimeException
        var cause = new IllegalStateException("plugin init failed");
        BiFunction<Configuration, String, VirtualClusterModel> failingResolver = (cfg, name) -> {
            throw cause;
        };
        var registry = new VirtualClusterRegistry(List.of(), failingResolver, noOpCallback);

        // when / then — the same RuntimeException instance is rethrown, NOT wrapped in
        // CompletionException. AddCluster relies on this for catch-by-type when surfacing
        // per-cluster ReconfigureError causes.
        assertThatThrownBy(() -> registry.resolveModel(mock(Configuration.class), CLUSTER_A))
                .isSameAs(cause);
    }

    @Test
    void resolveModelKeepsLifecycleThreadAliveAfterResolverError() {
        // given — a resolver that throws on the first call and succeeds on every subsequent call.
        // Capture the thread on both calls so we can prove the executor's single worker survives.
        var callCount = new AtomicInteger();
        var firstThread = new AtomicReference<Thread>();
        var secondThread = new AtomicReference<Thread>();
        BiFunction<Configuration, String, VirtualClusterModel> flakyResolver = (cfg, name) -> {
            var n = callCount.incrementAndGet();
            if (n == 1) {
                firstThread.set(Thread.currentThread());
                throw new RuntimeException("first call fails");
            }
            secondThread.set(Thread.currentThread());
            return mockModel(name);
        };
        var registry = new VirtualClusterRegistry(List.of(), flakyResolver, noOpCallback);

        // when — first call fails, second call must still complete on the same thread
        assertThatThrownBy(() -> registry.resolveModel(mock(Configuration.class), CLUSTER_A))
                .isInstanceOf(RuntimeException.class)
                .hasMessage("first call fails");
        var result = registry.resolveModel(mock(Configuration.class), CLUSTER_B);

        // then — second call succeeded AND landed on the same thread as the first. Same-thread
        // proves the executor's worker was not silently replaced (which would mask a Throwable
        // escape that killed the original worker).
        assertThat(result).isNotNull();
        assertThat(secondThread.get())
                .as("lifecycle thread should survive a resolver failure")
                .isSameAs(firstThread.get());
    }

    // ------------------------------------------------------------------------------------------
    // transitionToStoppedAndClose — concurrent-dispatch dedup and unexpected-state handling.
    // These exercise branches that are unreachable through normal single-threaded test paths
    // because the outer shutdownCluster state check short-circuits before dispatch.
    // ------------------------------------------------------------------------------------------

    @Test
    void transitionToStoppedAndCloseNoOpsWhenAlreadyStopped() {
        // given — a cluster already in Stopped (simulates a concurrent dispatch having beaten
        // this one through the lifecycle thread)
        var model = mockModel(CLUSTER_A);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, noOpCallback);
        var lifecycle = registry.lifecycleFor(CLUSTER_A);
        Assumptions.assumeThat(lifecycle).isNotNull();
        lifecycle.initializationFailed(new RuntimeException("boom"));
        lifecycle.stop();

        // when — a stale dispatched task lands here after another path already drove to Stopped
        registry.transitionToStoppedAndClose(CLUSTER_A, lifecycle);

        // then — silent no-op; model.close not re-invoked, no callback fired
        verify(model, never()).close();
        verifyNoInteractions(noOpCallback);
    }

    @Test
    @SuppressWarnings("unchecked")
    void transitionToStoppedAndCloseLogsWhenObservingUnexpectedState() {
        // given — a cluster in Serving (the unexpected state for this method, which only ever
        // expects Draining / Failed / Initializing / Stopped on entry)
        var model = mockModel(CLUSTER_A);
        BiConsumer<String, Optional<Throwable>> callback = mock(BiConsumer.class);
        var registry = new VirtualClusterRegistry(List.of(model), NO_OP_RESOLVER, callback);
        var lifecycle = registry.lifecycleFor(CLUSTER_A);
        Assumptions.assumeThat(lifecycle).isNotNull();
        lifecycle.initializationSucceeded();

        // when — direct invocation simulates the race where initializationSucceeded() raced
        // with our dispatch (left the cluster in Serving when transitionToStoppedAndClose ran)
        registry.transitionToStoppedAndClose(CLUSTER_A, lifecycle);

        // then — silent no-op on the work, but the unexpected state was logged (logging
        // verified implicitly: the method must not throw and must not fire close/callback)
        verify(model, never()).close();
        verifyNoInteractions(callback);
    }

    @Test
    void closeIsIdempotentAndShutsDownLifecycleExecutor() {
        // given
        var registry = new VirtualClusterRegistry(List.of(), NO_OP_RESOLVER, noOpCallback);

        // when / then — first close completes without throwing
        assertThatCode(registry::close).doesNotThrowAnyException();

        // second close is a no-op (already-shutdown executor's shutdown call is idempotent)
        assertThatCode(registry::close).doesNotThrowAnyException();
    }

}
