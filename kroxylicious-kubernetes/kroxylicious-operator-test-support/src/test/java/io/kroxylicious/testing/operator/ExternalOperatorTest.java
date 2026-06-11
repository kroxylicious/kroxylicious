/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator;

import java.util.function.UnaryOperator;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NamespaceableResource;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@EnableKubernetesMockClient(crud = true)
class ExternalOperatorTest {

    private static final String NAMESPACE = "test-ns";

    KubernetesClient client;

    @Test
    void updateStatusThrowsWhenResourceNotFound() {
        var stub = new ExternalOperator(client, NAMESPACE);

        assertThatNullPointerException()
                .isThrownBy(() -> stub.updateStatus(KafkaProxy.class, "missing", r -> r))
                .withMessageContaining("KafkaProxy")
                .withMessageContaining("missing");
    }

    @Test
    @SuppressWarnings("unchecked")
    void updateStatusFetchesFreshResourceBeforeApplyingMutator() {
        // Given
        KubernetesClient mockClient = mock(KubernetesClient.class);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockMixedOp = mock(MixedOperation.class);
        NonNamespaceOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockNonNamespaced = mock(NonNamespaceOperation.class);
        Resource<KafkaProxy> mockResource = mock(Resource.class);
        NamespaceableResource<KafkaProxy> mockNamespaceable = mock(NamespaceableResource.class);

        KafkaProxy fresh = new KafkaProxyBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-proxy").withResourceVersion("42").build())
                .build();
        KafkaProxy patched = new KafkaProxyBuilder()
                .withMetadata(new ObjectMetaBuilder().withName("my-proxy").withResourceVersion("43").build())
                .build();

        when(mockClient.resources(KafkaProxy.class)).thenReturn(mockMixedOp);
        when(mockMixedOp.inNamespace(NAMESPACE)).thenReturn(mockNonNamespaced);
        when(mockNonNamespaced.withName("my-proxy")).thenReturn(mockResource);
        when(mockResource.get()).thenReturn(fresh);
        when(mockClient.resource(fresh)).thenReturn(mockNamespaceable);
        when(mockNamespaceable.inNamespace(NAMESPACE)).thenReturn(mockNamespaceable);
        when(mockNamespaceable.patchStatus()).thenReturn(patched);

        UnaryOperator<KafkaProxy> mutator = mock(UnaryOperator.class);
        when(mutator.apply(fresh)).thenReturn(fresh);

        // When
        KafkaProxy result = new ExternalOperator(mockClient, NAMESPACE).updateStatus(KafkaProxy.class, "my-proxy", mutator);

        // Then — mutator receives the re-fetched resource, and the result is what patchStatus returned
        verify(mutator).apply(fresh);
        verify(mockNamespaceable).patchStatus();
        assertThat(result).isSameAs(patched);
    }
}