/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.kroxylicious.proxy.config.OnFailure;
import io.kroxylicious.proxy.model.VirtualClusterModel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ConfigurationChangeHandlerTest {

    @Mock
    private ChangeDetector changeDetector;

    @Mock
    private VirtualClusterManager virtualClusterManager;

    @Mock
    private ConfigurationChangeContext changeContext;

    private ConfigurationChangeHandler handler;

    @BeforeEach
    void setUp() {
        when(changeDetector.getName()).thenReturn("test-detector");
        handler = new ConfigurationChangeHandler(List.of(changeDetector), virtualClusterManager);
    }

    @Test
    void defaultOverloadShouldDelegateWithRollback() {
        // Given: no changes detected
        when(changeContext.oldModels()).thenReturn(List.of());
        when(changeContext.newModels()).thenReturn(List.of());
        when(changeDetector.detectChanges(any())).thenReturn(new ChangeResult(List.of(), List.of(), List.of()));

        // When
        CompletableFuture<Void> result = handler.handleConfigurationChange(changeContext);

        // Then
        assertThat(result).isCompleted();
    }

    @Test
    void shouldNotRollbackWhenOnFailureIsContinue() {
        // Given: a modification that will fail
        VirtualClusterModel model = org.mockito.Mockito.mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn("cluster1");
        when(changeContext.oldModels()).thenReturn(List.of(model));
        when(changeContext.newModels()).thenReturn(List.of(model));
        when(changeDetector.detectChanges(any())).thenReturn(new ChangeResult(List.of(), List.of(), List.of("cluster1")));
        when(virtualClusterManager.restartVirtualCluster(eq("cluster1"), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("restart failed")));

        // When
        CompletableFuture<Void> result = handler.handleConfigurationChange(changeContext, OnFailure.CONTINUE);

        // Then: the future should complete exceptionally but NOT trigger rollback
        assertThat(result).isCompletedExceptionally();
        // With CONTINUE, removeVirtualCluster (rollback operation) should NOT be called
        verify(virtualClusterManager, never()).removeVirtualCluster(eq("cluster1"), any(), any());
    }

    @Test
    void shouldNotRollbackWhenOnFailureIsTerminate() {
        // Given: a modification that will fail
        VirtualClusterModel model = org.mockito.Mockito.mock(VirtualClusterModel.class);
        when(model.getClusterName()).thenReturn("cluster1");
        when(changeContext.oldModels()).thenReturn(List.of(model));
        when(changeContext.newModels()).thenReturn(List.of(model));
        when(changeDetector.detectChanges(any())).thenReturn(new ChangeResult(List.of(), List.of(), List.of("cluster1")));
        when(virtualClusterManager.restartVirtualCluster(eq("cluster1"), any(), any(), any()))
                .thenReturn(CompletableFuture.failedFuture(new RuntimeException("restart failed")));

        // When
        CompletableFuture<Void> result = handler.handleConfigurationChange(changeContext, OnFailure.TERMINATE);

        // Then: the future should complete exceptionally but NOT trigger rollback
        assertThat(result).isCompletedExceptionally();
        verify(virtualClusterManager, never()).removeVirtualCluster(eq("cluster1"), any(), any());
    }

    @Test
    void shouldCompleteSuccessfullyWhenNoChangesDetected() {
        // Given
        when(changeContext.oldModels()).thenReturn(List.of());
        when(changeContext.newModels()).thenReturn(List.of());
        when(changeDetector.detectChanges(any())).thenReturn(new ChangeResult(List.of(), List.of(), List.of()));

        // When
        CompletableFuture<Void> result = handler.handleConfigurationChange(changeContext, OnFailure.CONTINUE);

        // Then
        assertThat(result).isCompleted();
    }
}
