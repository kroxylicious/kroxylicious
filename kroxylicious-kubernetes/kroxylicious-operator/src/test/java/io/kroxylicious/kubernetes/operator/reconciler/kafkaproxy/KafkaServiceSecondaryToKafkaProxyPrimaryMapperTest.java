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
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaServiceSecondaryToKafkaProxyPrimaryMapperTest {

    @Test
    void kafkaServiceToProxyMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaService kafkaServiceRef = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", kafkaServiceRef);

        KubernetesResourceList<KafkaProxy> mockProxyList = MapperTestSupport.mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(cluster));

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaServiceRef);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void kafkaServiceToProxyMapperIgnoresServiceWithStaleStatus() {
        // given
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaService kafkaServiceRef = MapperTestSupport.buildKafkaService("ref", 5L, 3L);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", kafkaServiceRef);

        KubernetesResourceList<KafkaProxy> mockProxyList = MapperTestSupport.mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(cluster));

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaServiceRef);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void kafkaServiceToProxyMapperHandlesSharedKafkaService() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaService kafkaServiceRef = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
        KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
        KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
        VirtualKafkaCluster proxy1cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaServiceRef);
        VirtualKafkaCluster proxy2cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2cluster", kafkaServiceRef);

        KubernetesResourceList<KafkaProxy> mockProxyList = MapperTestSupport.mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy1, proxy2));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(proxy1cluster, proxy2cluster));

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafkaServiceRef);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
    }

    @Test
    void kafkaServiceToProxyMapperHandlesOrphanKafkaService() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaService orphanKafkaClusterRed = MapperTestSupport.buildKafkaService("orphan", 1L, 1L);
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", MapperTestSupport.buildKafkaService("ref", 1L, 1L));

        KubernetesResourceList<KafkaProxy> mockProxyList = MapperTestSupport.mockKafkaProxyListOperation(client);
        when(mockProxyList.getItems()).thenReturn(List.of(proxy));

        KubernetesResourceList<VirtualKafkaCluster> mockClusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
        when(mockClusterList.getItems()).thenReturn(List.of(cluster));

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(orphanKafkaClusterRed);
        assertThat(primaryResourceIDs).isEmpty();
    }

}