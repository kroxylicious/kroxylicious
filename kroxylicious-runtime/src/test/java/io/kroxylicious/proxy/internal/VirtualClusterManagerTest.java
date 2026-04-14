/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class VirtualClusterManagerTest {

    private static final String CLUSTER_A = "cluster-a";

    @SuppressWarnings("unchecked")
    private final BiConsumer<String, Optional<Throwable>> noOpCallback = mock(BiConsumer.class);

    private VirtualClusterManager vcm;

    private static VirtualClusterModel mockModel(String name) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        return model;
    }

    @BeforeEach
    void setUp() {
        vcm = new VirtualClusterManager(List.of(mockModel(CLUSTER_A)), noOpCallback);
    }

    @Test
    void shouldCreateLifecycleManagerInInitializingState() {
        // when
        var manager = vcm.lifecycleManagerFor(CLUSTER_A);

        // then
        assertThat(manager).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Initializing.class);
    }

    @Test
    void shouldCreateLifecycleManagerForEachModel() {
        // given
        var multiVcm = new VirtualClusterManager(
                List.of(mockModel("cluster-a"), mockModel("cluster-b")),
                noOpCallback);

        // when/then
        assertThat(multiVcm.lifecycleManagerFor("cluster-a")).isNotNull();
        assertThat(multiVcm.lifecycleManagerFor("cluster-b")).isNotNull();
    }

    @Test
    void shouldReturnNullForUnknownCluster() {
        // when
        var result = vcm.lifecycleManagerFor("nonexistent");

        // then
        assertThat(result).isNull();
    }

    @Test
    void shouldExposeVirtualClusterModels() {
        // given
        var modelA = mockModel("cluster-a");
        var modelB = mockModel("cluster-b");
        var multiVcm = new VirtualClusterManager(List.of(modelA, modelB), noOpCallback);

        // when
        var models = multiVcm.virtualClusterModels();

        // then
        assertThat(models).containsExactly(modelA, modelB);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullModels() {
        assertThatThrownBy(() -> new VirtualClusterManager(null, noOpCallback))
                .isInstanceOf(NullPointerException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void shouldRejectNullCallback() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterManager(virtualClusterModels, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void shouldRejectDuplicateClusterNames() {
        List<VirtualClusterModel> virtualClusterModels = List.of(mockModel(CLUSTER_A), mockModel(CLUSTER_A));
        assertThatThrownBy(() -> new VirtualClusterManager(
                virtualClusterModels,
                noOpCallback))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining(CLUSTER_A);
    }

    // Per-VC lifecycle transitions

    @Test
    void shouldTransitionNamedClusterToServing() {
        // when
        vcm.initializationSucceeded(CLUSTER_A);

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Serving.class);
    }

    @Test
    void shouldNotFireCallbackOnInitializationSuccess() {
        // when
        vcm.initializationSucceeded(CLUSTER_A);

        // then
        verifyNoInteractions(noOpCallback);
    }

    @Test
    void shouldTransitionToStoppedOnInitializationFailure() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcm.initializationFailed(CLUSTER_A, cause);

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackOnInitializationFailure() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcm.initializationFailed(CLUSTER_A, cause);

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.of(cause));
    }

    @Test
    void shouldRetainFailureCauseInStoppedState() {
        // given
        var cause = new RuntimeException("filter init failed");

        // when
        vcm.initializationFailed(CLUSTER_A, cause);

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOfSatisfying(VirtualClusterLifecycleState.Stopped.class,
                        stopped -> assertThat(stopped.priorFailureCause()).isSameAs(cause));
    }

    @Test
    void shouldThrowForUnknownClusterOnInitializationSucceeded() {
        // when/then
        assertThatThrownBy(() -> vcm.initializationSucceeded("nonexistent"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shouldThrowForUnknownClusterOnInitializationFailed() {
        // when/then
        RuntimeException boom = new RuntimeException("boom");
        assertThatThrownBy(() -> vcm.initializationFailed("nonexistent", boom))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // Bulk shutdown transitions

    @Test
    void shouldTransitionServingToDrainingOnBulkDrain() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);

        // when
        vcm.transitionAllToDraining();

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Draining.class);
    }

    @Test
    void shouldTransitionInitializingToStoppedOnBulkDrain() {
        // when
        vcm.transitionAllToDraining();

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackForInitializingStoppedDuringBulkDrain() {
        // when
        vcm.transitionAllToDraining();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldNotFireCallbackForServingToDrainingOnBulkDrain() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);

        // when
        vcm.transitionAllToDraining();

        // then
        verifyNoInteractions(noOpCallback);
    }

    @Test
    void shouldTransitionDrainingToStoppedOnBulkStop() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);
        vcm.transitionAllToDraining();

        // when
        vcm.transitionAllToStopped();

        // then
        assertThat(vcm.lifecycleManagerFor(CLUSTER_A)).isNotNull()
                .extracting(VirtualClusterLifecycleManager::state)
                .isInstanceOf(VirtualClusterLifecycleState.Stopped.class);
    }

    @Test
    void shouldFireCallbackWithEmptyCauseOnBulkStop() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);
        vcm.transitionAllToDraining();

        // when
        vcm.transitionAllToStopped();

        // then
        verify(noOpCallback).accept(CLUSTER_A, Optional.empty());
    }

    @Test
    void shouldReturnTrueWhenAllClustersStopped() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);
        vcm.transitionAllToDraining();

        // when
        var allStopped = vcm.transitionAllToStopped();

        // then
        assertThat(allStopped).isTrue();
    }

    @Test
    void shouldReturnFalseWhenNotAllClustersStopped() {
        // given
        vcm.initializationSucceeded(CLUSTER_A);

        // when
        var allStopped = vcm.transitionAllToStopped();

        // then
        assertThat(allStopped).isFalse();
    }
}
