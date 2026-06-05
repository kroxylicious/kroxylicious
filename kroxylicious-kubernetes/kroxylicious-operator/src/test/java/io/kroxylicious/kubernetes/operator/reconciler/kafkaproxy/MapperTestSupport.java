/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.function.Predicate;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MapperTestSupport {

    public static void stubPrimaryCache(EventSourceContext<KafkaProxy> context, KafkaProxy... proxies) {
        IndexerResourceCache<KafkaProxy> primaryCache = mock();
        when(primaryCache.list(any(), any())).thenAnswer(invocation -> {
            Predicate<KafkaProxy> predicate = invocation.getArgument(1);
            return Stream.of(proxies).filter(predicate);
        });
        when(context.getPrimaryCache()).thenReturn(primaryCache);
    }

    public static void stubFailingListOperationClient(EventSourceContext<KafkaProxy> context) {
        KubernetesClient client = mock();
        when(context.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        when(mockOperation.list()).thenThrow(new KubernetesClientException("transient API server failure"));
    }

    public static EventSourceContext<KafkaProxy> mockContextContaining(KafkaProxy... proxies) {
        EventSourceContext<KafkaProxy> context = mock();
        stubPrimaryCache(context, proxies);
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
