/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.Namespace;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LocalKroxyliciousOperatorExtensionLifecycleTest {

    private final LocallyRunningOperatorRbacHandler rbacHandler = mock(LocallyRunningOperatorRbacHandler.class);
    private final LocallyRunOperatorExtension locallyRunOperatorExtension = mock(LocallyRunOperatorExtension.class);
    private final ExtensionContext context = mock(ExtensionContext.class);

    private final LocalKroxyliciousOperatorExtension extension = new LocalKroxyliciousOperatorExtension(
            LocalKroxyliciousOperatorExtension.builder()
                    .withReconciler((KafkaProxy resource, Context<KafkaProxy> ctx) -> UpdateControl.noUpdate()),
            () -> rbacHandler,
            handler -> locallyRunOperatorExtension,
            () -> {
            });

    @Test
    void beforeAllSetsUpRbacThenStartsOperator() throws Exception {
        extension.beforeAll(context);

        InOrder order = inOrder(rbacHandler, locallyRunOperatorExtension);
        order.verify(rbacHandler).beforeEach(context);
        order.verify(locallyRunOperatorExtension).beforeAll(context);
    }

    @Test
    void afterAllStopsOperatorThenCleansUpRbac() throws Exception {
        extension.beforeAll(context);

        extension.afterAll(context);

        InOrder order = inOrder(locallyRunOperatorExtension, rbacHandler);
        order.verify(locallyRunOperatorExtension).afterAll(context);
        order.verify(rbacHandler).afterEach(context);
        order.verify(rbacHandler).afterAll(context);
    }

    @Test
    void afterAllIsIdempotentIfBeforeAllWasNeverCalled() {
        extension.afterAll(context);

        verify(locallyRunOperatorExtension, never()).afterAll(context);
        verify(rbacHandler, never()).afterEach(context);
    }

    @Test
    void afterEachIsSafeIfBeforeAllWasNeverCalled() {
        assertThatNoException().isThrownBy(() -> extension.afterEach(context));
        // testActor is null — nothing to verify, just must not throw
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void afterEachCleansUpAllStandardResourceTypes() throws Exception {
        // Given
        var setup = extensionWithMockActor(defaultBuilder(), List.of());

        // When
        setup.extension().afterEach(context);

        // Then
        ArgumentCaptor<Class> captor = ArgumentCaptor.forClass(Class.class);
        verify(setup.actor(), atLeastOnce()).resources(captor.capture());
        assertThat(captor.getAllValues()).containsExactlyInAnyOrder(
                VirtualKafkaCluster.class, KafkaProxy.class, KafkaProxyIngress.class,
                KafkaService.class, KafkaProtocolFilter.class, Secret.class, ConfigMap.class);
    }

    @Test
    @SuppressWarnings({ "unchecked", "rawtypes" })
    void afterEachDeletesFoundResources() throws Exception {
        // Given: each standard type has one uniquely named resource so failures identify the missing type
        var localRbac = mock(LocallyRunningOperatorRbacHandler.class);
        var localOp = mock(LocallyRunOperatorExtension.class);
        var ext = new LocalKroxyliciousOperatorExtension(defaultBuilder(), () -> localRbac, handler -> localOp, () -> {
        });

        LocallyRunningOperatorRbacHandler.TestActor mockActor = mock(LocallyRunningOperatorRbacHandler.TestActor.class);
        when(localRbac.testActor(any())).thenReturn(mockActor);

        List<Class<? extends HasMetadata>> standardTypes = List.of(
                VirtualKafkaCluster.class, KafkaProxy.class, KafkaProxyIngress.class,
                KafkaService.class, KafkaProtocolFilter.class, Secret.class, ConfigMap.class);

        Map<Class<? extends HasMetadata>, HasMetadata> itemsByType = new LinkedHashMap<>();
        for (Class<? extends HasMetadata> type : standardTypes) {
            HasMetadata item = mock(HasMetadata.class, type.getSimpleName() + "-item");
            itemsByType.put(type, item);
            NonNamespaceOperation mockOp = mock(NonNamespaceOperation.class);
            KubernetesResourceList mockList = mock(KubernetesResourceList.class);
            when(mockList.getItems()).thenReturn(List.of(item));
            when(mockOp.list()).thenReturn(mockList);
            when(mockActor.resources(type)).thenReturn(mockOp);
        }

        ext.beforeAll(context);

        // When
        ext.afterEach(context);

        // Then: every found resource was deleted — named mocks identify the type in failure messages
        ArgumentCaptor<HasMetadata> deleteCaptor = ArgumentCaptor.forClass(HasMetadata.class);
        verify(mockActor, atLeastOnce()).delete(deleteCaptor.capture());
        assertThat(deleteCaptor.getAllValues())
                .containsExactlyInAnyOrder(itemsByType.values().toArray(HasMetadata[]::new));
    }

    @Test
    void afterEachAlsoCleansUpAdditionalCleanupTypes() throws Exception {
        // Given
        var setup = extensionWithMockActor(defaultBuilder().withAdditionalCleanupTypes(Namespace.class), List.of());

        // When
        setup.extension().afterEach(context);

        // Then
        verify(setup.actor()).resources(Namespace.class);
    }

    // ---- helpers ----

    private LocalKroxyliciousOperatorExtension.Builder defaultBuilder() {
        return LocalKroxyliciousOperatorExtension.builder()
                .withReconciler((KafkaProxy resource, Context<KafkaProxy> ctx) -> UpdateControl.noUpdate());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private ExtensionWithActor extensionWithMockActor(LocalKroxyliciousOperatorExtension.Builder builder,
                                                      List<? extends HasMetadata> itemsPerType)
            throws Exception {
        var localRbac = mock(LocallyRunningOperatorRbacHandler.class);
        var localOp = mock(LocallyRunOperatorExtension.class);
        var ext = new LocalKroxyliciousOperatorExtension(builder, () -> localRbac, handler -> localOp, () -> {
        });

        LocallyRunningOperatorRbacHandler.TestActor mockActor = mock(LocallyRunningOperatorRbacHandler.TestActor.class);
        NonNamespaceOperation mockOp = mock(NonNamespaceOperation.class);
        KubernetesResourceList mockList = mock(KubernetesResourceList.class);
        when(mockList.getItems()).thenReturn(itemsPerType);
        when(mockOp.list()).thenReturn(mockList);
        when(mockActor.resources(any())).thenReturn(mockOp);
        when(localRbac.testActor(any())).thenReturn(mockActor);

        ext.beforeAll(context);
        return new ExtensionWithActor(ext, mockActor);
    }

    private record ExtensionWithActor(LocalKroxyliciousOperatorExtension extension,
                                      LocallyRunningOperatorRbacHandler.TestActor actor) {}
}
