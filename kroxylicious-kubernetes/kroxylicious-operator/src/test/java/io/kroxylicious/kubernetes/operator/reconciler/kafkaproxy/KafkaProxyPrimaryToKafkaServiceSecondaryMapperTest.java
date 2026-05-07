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
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProxyPrimaryToKafkaServiceSecondaryMapperTest {

    @Test
    void proxyToKafkaServiceMapper() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaService> clusterRefListMock = MapperTestSupport.mockKafkaServiceListOperation(client);

        KafkaService kafkaServiceRef = MapperTestSupport.buildKafkaService("ref", 1L, 1L);

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaServiceRef));

        VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", kafkaServiceRef);

        when(clusterListMock.getItems()).thenReturn(List.of(cluster));

        PrimaryToSecondaryMapper<KafkaProxy> mapper = new KafkaProxyPrimaryToKafkaServiceSecondaryMapper(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(kafkaServiceRef));
    }

    @Test
    void proxyToKafkaServiceMapperDistinguishesByProxy() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
        KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaService> clusterRefListMock = MapperTestSupport.mockKafkaServiceListOperation(client);

        KafkaService kafkaServiceRefProxy1 = MapperTestSupport.buildKafkaService("proxy1ref", 1L, 1L);
        KafkaService kafkaServiceRefProxy2 = MapperTestSupport.buildKafkaService("proxy2ref", 1L, 1L);

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaServiceRefProxy1, kafkaServiceRefProxy2));

        VirtualKafkaCluster clusterProxy1 = MapperTestSupport.buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaServiceRefProxy1);
        VirtualKafkaCluster clusterProxy2 = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2cluster", kafkaServiceRefProxy2);

        when(clusterListMock.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2));

        PrimaryToSecondaryMapper<KafkaProxy> mapper = new KafkaProxyPrimaryToKafkaServiceSecondaryMapper(eventSourceContext);

        assertThat(mapper.toSecondaryResourceIDs(proxy1)).containsExactly(ResourceID.fromResource(kafkaServiceRefProxy1));
        assertThat(mapper.toSecondaryResourceIDs(proxy2)).containsExactly(ResourceID.fromResource(kafkaServiceRefProxy2));
    }

    @Test
    void proxyToKafkaServiceMapperHandlesProxyWithMultipleRefs() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
        KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);

        KubernetesResourceList<KafkaService> clusterRefListMock = MapperTestSupport.mockKafkaServiceListOperation(client);

        KafkaService kafkaServiceRefProxy1 = MapperTestSupport.buildKafkaService("proxy1ref", 1L, 1L);

        KafkaService kafkaServiceRefProxy2a = MapperTestSupport.buildKafkaService("proxy2refa", 1L, 1L);
        KafkaService kafkaServiceRefProxy2b = MapperTestSupport.buildKafkaService("proxy2refb", 1L, 1L);

        when(clusterRefListMock.getItems()).thenReturn(List.of(kafkaServiceRefProxy1, kafkaServiceRefProxy2a, kafkaServiceRefProxy2b));

        VirtualKafkaCluster clusterProxy1 = MapperTestSupport.buildVirtualKafkaCluster(proxy1, "proxy1cluster", kafkaServiceRefProxy1);
        VirtualKafkaCluster clusterProxy2a = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2clustera", kafkaServiceRefProxy2a);
        VirtualKafkaCluster clusterProxy2b = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2clusterb", kafkaServiceRefProxy2b);

        when(clusterListMock.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2a, clusterProxy2b));

        PrimaryToSecondaryMapper<KafkaProxy> mapper = new KafkaProxyPrimaryToKafkaServiceSecondaryMapper(eventSourceContext);

        assertThat(mapper.toSecondaryResourceIDs(proxy1))
                .containsExactly(ResourceID.fromResource(kafkaServiceRefProxy1));

        assertThat(mapper.toSecondaryResourceIDs(proxy2))
                .containsExactly(ResourceID.fromResource(kafkaServiceRefProxy2a), ResourceID.fromResource(kafkaServiceRefProxy2b));
    }

    @Test
    void proxyToKafkaServiceMapperIgnoresDanglingKafkaService() {
        EventSourceContext<KafkaProxy> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");

        KubernetesResourceList<VirtualKafkaCluster> clusterListMock = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
        MapperTestSupport.mockKafkaServiceListOperation(client);

        VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", MapperTestSupport.buildKafkaService("dangle", 1L, 1L));

        when(clusterListMock.getItems()).thenReturn(List.of(cluster));

        PrimaryToSecondaryMapper<KafkaProxy> mapper = new KafkaProxyPrimaryToKafkaServiceSecondaryMapper(eventSourceContext);
        Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);
        assertThat(secondaryResourceIDs).isEmpty();
    }

}