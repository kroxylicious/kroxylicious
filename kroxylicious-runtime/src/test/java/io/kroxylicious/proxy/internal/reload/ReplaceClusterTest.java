/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ReplaceClusterTest {

    private static final String NAME = "cluster-modify";

    private final VirtualClusterRegistry vcr = mock(VirtualClusterRegistry.class);
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);

    @Test
    void shouldDriveRemoveThenAddOnHappyPath() {
        var oldGateway = mock(EndpointGateway.class);
        var newGateway = mock(EndpointGateway.class);
        var oldModel = modelWithGateway(NAME, oldGateway);
        var newModel = modelWithGateway(NAME, newGateway);
        stubRemoveSucceeds();
        stubDeregisterSucceeds();
        stubAddBookkeepingSucceeds();
        stubRegisterSucceeds();

        var result = new ReplaceCluster(oldModel, newModel, vcr, endpointRegistry).apply();

        assertThat(result).isEmpty();
        var inOrder = inOrder(vcr, endpointRegistry);
        inOrder.verify(vcr).removeVirtualCluster(NAME);
        inOrder.verify(endpointRegistry).deregisterVirtualCluster(oldGateway);
        inOrder.verify(vcr).addVirtualCluster(newModel);
        inOrder.verify(endpointRegistry).registerVirtualCluster(newGateway);
        inOrder.verify(vcr).initializationSucceeded(NAME);
    }

    @Test
    void shouldShortCircuitWhenRemoveFails() {
        // If the remove half can't complete, we don't proceed to the add. One error per
        // failing cluster — not a cascade.
        var oldGateway = mock(EndpointGateway.class);
        var newGateway = mock(EndpointGateway.class);
        var oldModel = modelWithGateway(NAME, oldGateway);
        var newModel = modelWithGateway(NAME, newGateway);
        var removeCause = new IllegalStateException("simulated drain failure");
        when(vcr.removeVirtualCluster(NAME)).thenReturn(CompletableFuture.failedFuture(removeCause));

        var result = new ReplaceCluster(oldModel, newModel, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(removeCause);
        });
        verify(vcr, never()).addVirtualCluster(any(VirtualClusterModel.class));
        verify(endpointRegistry, never()).registerVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void shouldReportAddErrorWhenRemoveSucceedsButAddFails() {
        // Remove half completes cleanly; bind on the new model fails. ReconfigureError carries
        // the add cause (the cluster's offline because of the failed add, not anything in the
        // remove). The cluster ends up in STOPPED — same shape as a pure-add bind failure.
        var oldGateway = mock(EndpointGateway.class);
        var newGateway = mock(EndpointGateway.class);
        var oldModel = modelWithGateway(NAME, oldGateway);
        var newModel = modelWithGateway(NAME, newGateway);
        stubRemoveSucceeds();
        stubDeregisterSucceeds();
        stubAddBookkeepingSucceeds();
        var bindCause = new IllegalStateException("simulated bind failure");
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(bindCause));
        stubDeregisterSucceeds(); // for the AddCluster rollback path

        var result = new ReplaceCluster(oldModel, newModel, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(bindCause);
        });
        verify(vcr).removeVirtualCluster(NAME);
        verify(vcr).addVirtualCluster(newModel);
        verify(vcr).initializationFailed(NAME, bindCause);
    }

    @Test
    void clusterNameReflectsTheNewModel() {
        // The remove half operates on oldModel but the operation's identity (its slot in the
        // change-detection vocabulary) is the name shared by both — which both models report.
        var oldModel = modelWithGateway(NAME, mock(EndpointGateway.class));
        var newModel = modelWithGateway(NAME, mock(EndpointGateway.class));

        assertThat(new ReplaceCluster(oldModel, newModel, vcr, endpointRegistry).clusterName())
                .isEqualTo(NAME);
    }

    @Test
    void constructorRejectsNameMismatch() {
        // Defence in depth: the planner only emits ReplaceCluster pairs where both models share
        // a name, but the operation guards against a buggy planner that violates that.
        var oldModel = modelWithGateway("cluster-a", mock(EndpointGateway.class));
        var newModel = modelWithGateway("cluster-b", mock(EndpointGateway.class));

        assertThatThrownBy(() -> new ReplaceCluster(oldModel, newModel, vcr, endpointRegistry))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cluster-a")
                .hasMessageContaining("cluster-b");
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void constructorRejectsNullArguments() {
        var model = modelWithGateway(NAME, mock(EndpointGateway.class));
        assertThatThrownBy(() -> new ReplaceCluster(null, model, vcr, endpointRegistry))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReplaceCluster(model, null, vcr, endpointRegistry))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReplaceCluster(model, model, null, endpointRegistry))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> new ReplaceCluster(model, model, vcr, null))
                .isInstanceOf(NullPointerException.class);
    }

    private void stubRemoveSucceeds() {
        when(vcr.removeVirtualCluster(NAME)).thenReturn(CompletableFuture.completedFuture(null));
    }

    private void stubDeregisterSucceeds() {
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
    }

    private void stubAddBookkeepingSucceeds() {
        when(vcr.addVirtualCluster(any(VirtualClusterModel.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void stubRegisterSucceeds() {
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
    }

    private static VirtualClusterModel modelWithGateway(String name, EndpointGateway gateway) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        var gateways = new LinkedHashMap<String, EndpointGateway>();
        gateways.put("default", gateway);
        when(model.gateways()).thenReturn((Map) gateways);
        return model;
    }
}
