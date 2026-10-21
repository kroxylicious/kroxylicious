/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.List;
import java.util.function.Predicate;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.NonNamespaceOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RefTest {

    // @formatter:off
    KafkaProxy proxy = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName("test-proxy")
                .withNamespace("test-namespace")
            .endMetadata()
            .build();
    // @formatter:on

    @Mock
    EventSourceContext<KafkaProxy> eventSourceContext;
    @Mock
    KubernetesClient kubernetesClient;
    @Mock
    MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> vkcResourcesOp;
    @Mock
    NonNamespaceOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> namespacedVkcOp;
    @Mock
    KubernetesResourceList<VirtualKafkaCluster> vkcListing;

    @BeforeEach
    void setUp() {

    }

    @Test
    void directIsNullSafe() {
        // Given
        // @formatter:off
        VirtualKafkaCluster virtualKafkaCluster = new VirtualKafkaClusterBuilder()
                .withNewSpec()
                .endSpec()
                .build();
        // @formatter:on

        // When
        ProxyRelationship<VirtualKafkaCluster> vkcToProxy = Ref.direct(vkc -> vkc.getSpec().getProxyRef());
        Predicate<VirtualKafkaCluster> virtualKafkaClusterPredicate = vkcToProxy.referencingSecondaries(eventSourceContext, proxy);
        // Then
        assertThat(virtualKafkaClusterPredicate.test(virtualKafkaCluster)).isFalse();
    }

    @Test
    void viaIsNullSafe() {

        // Given
        // @formatter:off
        VirtualKafkaCluster virtualKafkaCluster = new VirtualKafkaClusterBuilder()
                .withNewSpec()
                .endSpec()
                .build();
        // @formatter:on

        ProxyRelationship<VirtualKafkaCluster> ref = Ref.direct(vkc -> vkc.getSpec().getProxyRef());

        // When
        Predicate<VirtualKafkaCluster> hasMetadataPredicate = ref.referencingSecondaries(eventSourceContext, proxy);

        // Then
        assertThat(hasMetadataPredicate.test(virtualKafkaCluster)).isFalse();
    }

    @Test
    void refersToIsNullSafe() {
        // Given
        // @formatter:off
        VirtualKafkaCluster virtualKafkaCluster = new VirtualKafkaClusterBuilder()
                .withNewSpec()
                    .withNewProxyRef()
                        .withName("test-proxy")
                    .endProxyRef()
                    // no target kafka service ref!
                .endSpec()
                .build();
        // @formatter:on
        when(vkcListing.getItems()).thenAnswer(i -> List.of(virtualKafkaCluster));
        when(namespacedVkcOp.list()).thenReturn(vkcListing);
        when(vkcResourcesOp.inNamespace("test-namespace")).thenReturn(namespacedVkcOp);
        when(kubernetesClient.resources(VirtualKafkaCluster.class)).thenReturn(vkcResourcesOp);
        when(eventSourceContext.getClient()).thenReturn(kubernetesClient);
        ProxyRelationship<VirtualKafkaCluster> vkcToProxy = Ref.direct(vkc -> vkc.getSpec().getProxyRef());
        ProxyRelationship<HasMetadata> ref = Ref.via(VirtualKafkaCluster.class, Ref.refersTo(vkc -> vkc.getSpec().getTargetKafkaServiceRef()), vkcToProxy);

        // When
        Predicate<HasMetadata> hasMetadataPredicate = ref.referencingSecondaries(eventSourceContext, proxy);

        // Then
        assertThat(hasMetadataPredicate.test(new VirtualKafkaClusterBuilder().build())).isFalse();
    }

    @Test
    void refersToAnyIsNullSafe() {

        // Given
        // @formatter:off
        VirtualKafkaCluster virtualKafkaCluster = new VirtualKafkaClusterBuilder()
                .withNewSpec()
                    .withNewProxyRef()
                        .withName("test-proxy")
                    .endProxyRef()
                    // no target kafka service ref!
                .endSpec()
                .build();
        // @formatter:on
        when(vkcListing.getItems()).thenAnswer(i -> List.of(virtualKafkaCluster));
        when(namespacedVkcOp.list()).thenReturn(vkcListing);
        when(vkcResourcesOp.inNamespace("test-namespace")).thenReturn(namespacedVkcOp);
        when(kubernetesClient.resources(VirtualKafkaCluster.class)).thenReturn(vkcResourcesOp);
        when(eventSourceContext.getClient()).thenReturn(kubernetesClient);

        ProxyRelationship<VirtualKafkaCluster> vkcToProxy = Ref.direct(vkc -> vkc.getSpec().getProxyRef());
        var ref = Ref.via(VirtualKafkaCluster.class, Ref.refersToAny(vkc -> vkc.getSpec().getFilterRefs()), vkcToProxy);

        // When
        Predicate<HasMetadata> hasMetadataPredicate = ref.referencingSecondaries(eventSourceContext, proxy);

        // Then
        assertThat(hasMetadataPredicate.test(virtualKafkaCluster)).isFalse();
    }

}
