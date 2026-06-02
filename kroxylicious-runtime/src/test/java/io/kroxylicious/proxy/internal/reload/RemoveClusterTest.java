/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.proxy.internal.reload;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.internal.VirtualClusterRegistry;
import io.kroxylicious.proxy.internal.net.EndpointGateway;
import io.kroxylicious.proxy.internal.net.EndpointRegistry;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RemoveClusterTest {

    private static final String NAME = "cluster-remove";

    private final VirtualClusterRegistry vcr = mock(VirtualClusterRegistry.class);
    private final EndpointRegistry endpointRegistry = mock(EndpointRegistry.class);

    @Test
    void shouldDriveLifecycleToStoppedThenDeregisterGateways() {
        var gateway = mock(EndpointGateway.class);
        var model = modelWith(NAME, gateway);
        when(vcr.removeVirtualCluster(NAME)).thenReturn(CompletableFuture.completedFuture(null));
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.completedStage(null));

        var result = new RemoveCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).isEmpty();
        var inOrder = inOrder(vcr, endpointRegistry);
        inOrder.verify(vcr).removeVirtualCluster(NAME);
        inOrder.verify(endpointRegistry).deregisterVirtualCluster(gateway);
    }

    @Test
    void shouldReportErrorIfRemoveVirtualClusterFailsAndSkipDeregisterStep() {
        var model = modelWith(NAME, mock(EndpointGateway.class));
        var cause = new IllegalStateException("drain failure");
        when(vcr.removeVirtualCluster(NAME)).thenReturn(CompletableFuture.failedFuture(cause));

        var result = new RemoveCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(cause);
        });
        verify(endpointRegistry, never()).deregisterVirtualCluster(any(EndpointGateway.class));
    }

    @Test
    void shouldReportErrorIfDeregisterFailsAfterSuccessfulRemove() {
        var gateway = mock(EndpointGateway.class);
        var model = modelWith(NAME, gateway);
        when(vcr.removeVirtualCluster(NAME)).thenReturn(CompletableFuture.completedFuture(null));
        var deregisterCause = new IllegalStateException("simulated unbind failure");
        when(endpointRegistry.deregisterVirtualCluster(any(EndpointGateway.class)))
                .thenReturn(CompletableFuture.failedStage(deregisterCause));

        var result = new RemoveCluster(model, vcr, endpointRegistry).apply();

        assertThat(result).hasValueSatisfying(e -> {
            assertThat(e.humanReadableIdentifier()).isEqualTo(NAME);
            assertThat(e.cause()).isSameAs(deregisterCause);
        });
        verify(vcr).removeVirtualCluster(NAME);
        verify(endpointRegistry).deregisterVirtualCluster(gateway);
    }

    private static VirtualClusterModel modelWith(String name, EndpointGateway gateway) {
        var model = mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn(name);
        when(model.gateways()).thenReturn((Map) Map.of("default", gateway));
        return model;
    }
}
