/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload.operations;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AddClusterTest {

    private static final String NAME = "cluster-add";

    private final VirtualClusterRegistry vcr = mock(VirtualClusterRegistry.class);
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);

    @Test
    void shouldRunBookkeepingThenBindThenTransitionToServing() {
        var model = modelWithGateways(NAME, "default");
        stubBookkeepingSucceeds();
        stubBindSucceeds();

        var result = new AddCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).isEmpty();
        var inOrder = inOrder(vcr, endpointRegistry);
        inOrder.verify(vcr).addVirtualCluster(model);
        inOrder.verify(endpointRegistry).registerVirtualCluster(any(EndpointGateway.class));
        inOrder.verify(vcr).initializationSucceeded(NAME);
        verify(vcr, never()).initializationFailed(anyString(), any());
        verify(endpointRegistry, never()).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void shouldReportErrorIfBookkeepingFailsAndSkipBindStep() {
        var model = modelWithGateways(NAME, "default");
        var cause = new IllegalStateException("bookkeeping failed");
        when(vcr.addVirtualCluster(model)).thenReturn(CompletableFuture.failedFuture(cause));

        var result = new AddCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(cause);
        });
        verify(endpointRegistry, never()).registerVirtualCluster(any(EndpointGateway.class));
        verify(vcr, never()).initializationSucceeded(anyString());
    }

    @Test
    void shouldRollbackAndReportBindCauseWhenBindFails() {
        var model = modelWithGateways(NAME, "default");
        stubBookkeepingSucceeds();
        var bindCause = new IllegalStateException("simulated bind failure");
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(bindCause));
        stubDeregisterSucceeds();

        var result = new AddCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(bindCause);
        });
        verify(vcr).initializationFailed(NAME, bindCause);
        verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
        verify(vcr, never()).initializationSucceeded(anyString());
    }

    @Test
    void shouldKeepBindCauseEvenWhenRollbackDeregisterAlsoFails() {
        // The deregister failure is logged but doesn't replace the trigger-cause
        var model = modelWithGateways(NAME, "default");
        stubBookkeepingSucceeds();
        var bindCause = new IllegalStateException("simulated bind failure");
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(bindCause));
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(new IllegalStateException("rollback failure")));

        var result = new AddCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> assertThat(e.cause()).isSameAs(bindCause));
        verify(endpointRegistry).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    private void stubBookkeepingSucceeds() {
        when(vcr.addVirtualCluster(any(VirtualClusterModel.class)))
                .thenReturn(CompletableFuture.completedFuture(null));
    }

    private void stubBindSucceeds() {
        when(endpointRegistry.registerVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
    }

    private void stubDeregisterSucceeds() {
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));
    }

    private static VirtualClusterModel modelWithGateways(String name, String... gatewayNames) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        var gateways = new LinkedHashMap<String, EndpointGateway>();
        for (var g : gatewayNames) {
            gateways.put(g, mock(EndpointGateway.class));
        }
        when(model.gateways()).thenReturn((Map) gateways);
        return model;
    }
}
