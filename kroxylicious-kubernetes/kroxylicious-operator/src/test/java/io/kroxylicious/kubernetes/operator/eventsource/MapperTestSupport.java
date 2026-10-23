/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.function.Predicate;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapperTestSupport {

    @SafeVarargs
    public static <P extends HasMetadata> void stubPrimaryCache(EventSourceContext<P> context, P... primaries) {
        IndexerResourceCache<P> primaryCache = mock();
        when(primaryCache.list(any(), any())).thenAnswer(invocation -> {
            Predicate<P> predicate = invocation.getArgument(1);
            return Stream.of(primaries).filter(predicate);
        });
        when(context.getPrimaryCache()).thenReturn(primaryCache);
    }

    public static <P extends HasMetadata> void stubFailingListOperationClient(EventSourceContext<P> context, Class<P> primaryType) {
        KubernetesClient client = mock();
        when(context.getClient()).thenReturn(client);
        MixedOperation<P, KubernetesResourceList<P>, Resource<P>> mockOperation = mock();
        when(client.resources(primaryType)).thenReturn(mockOperation);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        when(mockOperation.list()).thenThrow(new KubernetesClientException("transient API server failure"));
    }

    @SafeVarargs
    public static <P extends HasMetadata> EventSourceContext<P> mockContextContaining(P... primaries) {
        EventSourceContext<P> context = mock();
        stubPrimaryCache(context, primaries);
        return context;
    }

    public static KafkaProxy buildProxy(String name) {
        return proxyBuilder(name).build();
    }

    public static KafkaProxyBuilder proxyBuilder(String name) {
        return new KafkaProxyBuilder().withNewMetadata().withName(name).endMetadata();
    }

    public static VirtualKafkaClusterBuilder baseVirtualKafkaClusterBuilder(KafkaProxy kafkaProxy, String name, long generation, long observedGeneration) {
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                .withName(name)
                .withGeneration(generation)
                .endMetadata()
                .withNewSpec()
                .withNewProxyRef()
                .withName(name(kafkaProxy))
                .endProxyRef().endSpec()
                .editStatus().withObservedGeneration(observedGeneration)
                .endStatus();
    }

    public static KubernetesResourceList<KafkaProxy> mockKafkaProxyListOperation(KubernetesClient client) {
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);

        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        return mockList;
    }

    public static KubernetesResourceList<VirtualKafkaCluster> mockVirtualKafkaClusterListOperation(KubernetesClient client) {
        MixedOperation<VirtualKafkaCluster, KubernetesResourceList<VirtualKafkaCluster>, Resource<VirtualKafkaCluster>> clusterOperation = mock();
        when(client.resources(VirtualKafkaCluster.class)).thenReturn(clusterOperation);

        KubernetesResourceList<VirtualKafkaCluster> clusterList = mock();
        when(clusterOperation.list()).thenReturn(clusterList);
        when(clusterOperation.inNamespace(any())).thenReturn(clusterOperation);
        return clusterList;
    }

    public static KubernetesResourceList<KafkaService> mockKafkaServiceListOperation(KubernetesClient client) {
        MixedOperation<KafkaService, KubernetesResourceList<KafkaService>, Resource<KafkaService>> clusterRefOperation = mock();
        when(client.resources(KafkaService.class)).thenReturn(clusterRefOperation);

        KubernetesResourceList<KafkaService> clusterRefList = mock();
        when(clusterRefOperation.list()).thenReturn(clusterRefList);
        when(clusterRefOperation.inNamespace(any())).thenReturn(clusterRefOperation);
        return clusterRefList;
    }

    public static KubernetesResourceList<KafkaProtocolFilter> mockKafkaProtocolFilterListOperation(KubernetesClient client) {
        MixedOperation<KafkaProtocolFilter, KubernetesResourceList<KafkaProtocolFilter>, Resource<KafkaProtocolFilter>> filterOperation = mock();
        when(client.resources(KafkaProtocolFilter.class)).thenReturn(filterOperation);

        KubernetesResourceList<KafkaProtocolFilter> filterList = mock();
        when(filterOperation.list()).thenReturn(filterList);
        when(filterOperation.inNamespace(any())).thenReturn(filterOperation);
        return filterList;
    }

    public static KubernetesResourceList<KafkaProxyIngress> mockKafkaProxyIngressListOperation(KubernetesClient client) {
        MixedOperation<KafkaProxyIngress, KubernetesResourceList<KafkaProxyIngress>, Resource<KafkaProxyIngress>> ingressOperation = mock();
        when(client.resources(KafkaProxyIngress.class)).thenReturn(ingressOperation);

        KubernetesResourceList<KafkaProxyIngress> ingressList = mock();
        when(ingressOperation.list()).thenReturn(ingressList);
        when(ingressOperation.inNamespace(any())).thenReturn(ingressOperation);
        return ingressList;
    }

    public static KafkaProtocolFilter buildFilter(String name, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withGeneration(generation)
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(observedGeneration)
                .endStatus()
                .build();
        // @formatter:on
    }

    public static KafkaProxyIngress buildIngress(String name, String proxyName, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withGeneration(generation)
                .endMetadata()
                .withNewSpec()
                    .withNewProxyRef().withName(proxyName).endProxyRef()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(observedGeneration)
                .endStatus()
                .build();
        // @formatter:on
    }

    public static KafkaProxyIngress buildIngress(String name, String proxyName) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withNewProxyRef().withName(proxyName).endProxyRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    public static KafkaService buildKafkaService(String name, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                    .withGeneration(generation)
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(observedGeneration)
                .endStatus()
                .build();
        // @formatter:on
    }

    public static VirtualKafkaCluster buildVirtualKafkaCluster(KafkaProxy kafkaProxy, String name, KafkaService clusterRef) {
        return baseVirtualKafkaClusterBuilder(kafkaProxy, name, 1L, 1L)
                .editOrNewSpec()
                .withNewTargetKafkaServiceRef()
                .withName(name(clusterRef))
                .endTargetKafkaServiceRef()
                .endSpec()
                .build();
    }
}
