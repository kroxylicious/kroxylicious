/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import io.fabric8.kubernetes.client.dsl.Resource;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterToKafkaProxySecondaryToPrimaryMapperTest {

    @Test
    void clusterToProxyMapper() {
        // given
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void clusterToProxyMapperIgnoresClusterWithStaleStatus() {
        // given
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);
        MixedOperation<KafkaProxy, KubernetesResourceList<KafkaProxy>, Resource<KafkaProxy>> mockOperation = mock();
        when(client.resources(KafkaProxy.class)).thenReturn(mockOperation);
        KubernetesResourceList<KafkaProxy> mockList = mock();
        when(mockOperation.list()).thenReturn(mockList);
        when(mockOperation.inNamespace(any())).thenReturn(mockOperation);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        when(mockList.getItems()).thenReturn(List.of(proxy));
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 2L, 1L).build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

}