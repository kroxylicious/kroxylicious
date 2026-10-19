/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises the single generic primary&rarr;secondary mapper through the {@link Ref} relationships registered by
 * {@link KafkaProxyReconciler}: direct references ({@link KafkaProxyIngress}, {@link VirtualKafkaCluster}) and
 * transitive references via {@link VirtualKafkaCluster} ({@link KafkaProtocolFilter}, {@link KafkaService}).
 */
class ReferencingSecondaryMapperTest {

    private static final ProxyRelationship<VirtualKafkaCluster> VKC_TO_PROXY = Ref.direct(vkc -> vkc.getSpec().getProxyRef());
    private static final ProxyRelationship<KafkaProxyIngress> INGRESS_TO_PROXY = Ref.direct(ingress -> ingress.getSpec().getProxyRef());
    private static final ProxyRelationship<KafkaProtocolFilter> FILTER_TO_PROXY = Ref.via(VirtualKafkaCluster.class,
            Ref.refersToAny(vkc -> vkc.getSpec().getFilterRefs()), VKC_TO_PROXY);
    private static final ProxyRelationship<KafkaService> SERVICE_TO_PROXY = Ref.via(VirtualKafkaCluster.class,
            Ref.refersTo(vkc -> vkc.getSpec().getTargetKafkaServiceRef()), VKC_TO_PROXY);

    @Nested
    class DirectRelationships {

        @Test
        void proxyToIngressReturnsReferencingIngress() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KubernetesResourceList<KafkaProxyIngress> ingressList = MapperTestSupport.mockKafkaProxyIngressListOperation(client);
            KafkaProxyIngress ingress = MapperTestSupport.buildIngress("ingress", "proxy");
            KafkaProxyIngress ingressForAnotherProxy = MapperTestSupport.buildIngress("ingress2", "anotherProxy");
            when(ingressList.getItems()).thenReturn(List.of(ingress, ingressForAnotherProxy));
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            ReferencingSecondaryMapper<KafkaProxyIngress> mapper = new ReferencingSecondaryMapper<>(context, KafkaProxyIngress.class, INGRESS_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(ingress));
        }

        @Test
        void proxyToClusterReturnsReferencingCluster() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
            VirtualKafkaCluster clusterForAnotherProxy = MapperTestSupport
                    .baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("anotherproxy"), "anothercluster", 1L, 1L).build();
            when(clusterList.getItems()).thenReturn(List.of(cluster, clusterForAnotherProxy));
            ReferencingSecondaryMapper<VirtualKafkaCluster> mapper = new ReferencingSecondaryMapper<>(context, VirtualKafkaCluster.class, VKC_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
        }

        @Test
        void proxyToClusterReturnsEmptyWhenNoClusterReferencesProxy() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            VirtualKafkaCluster clusterForAnotherProxy = MapperTestSupport
                    .baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("anotherProxy"), "cluster", 1L, 1L).build();
            when(clusterList.getItems()).thenReturn(List.of(clusterForAnotherProxy));
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            ReferencingSecondaryMapper<VirtualKafkaCluster> mapper = new ReferencingSecondaryMapper<>(context, VirtualKafkaCluster.class, VKC_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).isEmpty();
        }
    }

    @Nested
    class TransitiveFilterRelationship {

        @Test
        void proxyToFilterReturnsReferencedExistingFilter() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaProtocolFilter> filterList = MapperTestSupport.mockKafkaProtocolFilterListOperation(client);
            KafkaProtocolFilter filter = MapperTestSupport.buildFilter("filter", 1L, 1L);
            when(filterList.getItems()).thenReturn(List.of(filter));
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L)
                    .editOrNewSpec().addNewFilterRef().withName("filter").endFilterRef().endSpec().build();
            when(clusterList.getItems()).thenReturn(List.of(cluster));
            ReferencingSecondaryMapper<KafkaProtocolFilter> mapper = new ReferencingSecondaryMapper<>(context, KafkaProtocolFilter.class, FILTER_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(filter));
        }

        @Test
        void proxyToFilterReturnsEmptyForClusterWithoutFilters() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaProtocolFilter> filterList = MapperTestSupport.mockKafkaProtocolFilterListOperation(client);
            when(filterList.getItems()).thenReturn(List.of(MapperTestSupport.buildFilter("filter", 1L, 1L)));
            VirtualKafkaCluster clusterWithoutFilters = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
            when(clusterList.getItems()).thenReturn(List.of(clusterWithoutFilters));
            ReferencingSecondaryMapper<KafkaProtocolFilter> mapper = new ReferencingSecondaryMapper<>(context, KafkaProtocolFilter.class, FILTER_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).isEmpty();
        }

        @Test
        void proxyToFilterIgnoresDanglingFilterRef() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaProtocolFilter> filterList = MapperTestSupport.mockKafkaProtocolFilterListOperation(client);
            when(filterList.getItems()).thenReturn(List.of());
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L)
                    .editOrNewSpec().addNewFilterRef().withName("ghost").endFilterRef().endSpec().build();
            when(clusterList.getItems()).thenReturn(List.of(cluster));
            ReferencingSecondaryMapper<KafkaProtocolFilter> mapper = new ReferencingSecondaryMapper<>(context, KafkaProtocolFilter.class, FILTER_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).isEmpty();
        }
    }

    @Nested
    class TransitiveKafkaServiceRelationship {

        @Test
        void proxyToKafkaServiceReturnsReferencedService() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaService> serviceList = MapperTestSupport.mockKafkaServiceListOperation(client);
            KafkaService kafkaService = MapperTestSupport.buildKafkaService("ref", 1L, 1L);
            when(serviceList.getItems()).thenReturn(List.of(kafkaService));
            VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", kafkaService);
            when(clusterList.getItems()).thenReturn(List.of(cluster));
            ReferencingSecondaryMapper<KafkaService> mapper = new ReferencingSecondaryMapper<>(context, KafkaService.class, SERVICE_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(kafkaService));
        }

        @Test
        void proxyToKafkaServiceDistinguishesByProxy() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
            KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaService> serviceList = MapperTestSupport.mockKafkaServiceListOperation(client);
            KafkaService serviceProxy1 = MapperTestSupport.buildKafkaService("proxy1ref", 1L, 1L);
            KafkaService serviceProxy2 = MapperTestSupport.buildKafkaService("proxy2ref", 1L, 1L);
            when(serviceList.getItems()).thenReturn(List.of(serviceProxy1, serviceProxy2));
            VirtualKafkaCluster clusterProxy1 = MapperTestSupport.buildVirtualKafkaCluster(proxy1, "proxy1cluster", serviceProxy1);
            VirtualKafkaCluster clusterProxy2 = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2cluster", serviceProxy2);
            when(clusterList.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2));
            ReferencingSecondaryMapper<KafkaService> mapper = new ReferencingSecondaryMapper<>(context, KafkaService.class, SERVICE_TO_PROXY);

            // When / Then
            assertThat(mapper.toSecondaryResourceIDs(proxy1)).containsExactly(ResourceID.fromResource(serviceProxy1));
            assertThat(mapper.toSecondaryResourceIDs(proxy2)).containsExactly(ResourceID.fromResource(serviceProxy2));
        }

        @Test
        void proxyToKafkaServiceHandlesProxyWithMultipleRefs() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
            KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaService> serviceList = MapperTestSupport.mockKafkaServiceListOperation(client);
            KafkaService serviceProxy1 = MapperTestSupport.buildKafkaService("proxy1ref", 1L, 1L);
            KafkaService serviceProxy2a = MapperTestSupport.buildKafkaService("proxy2refa", 1L, 1L);
            KafkaService serviceProxy2b = MapperTestSupport.buildKafkaService("proxy2refb", 1L, 1L);
            when(serviceList.getItems()).thenReturn(List.of(serviceProxy1, serviceProxy2a, serviceProxy2b));
            VirtualKafkaCluster clusterProxy1 = MapperTestSupport.buildVirtualKafkaCluster(proxy1, "proxy1cluster", serviceProxy1);
            VirtualKafkaCluster clusterProxy2a = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2clustera", serviceProxy2a);
            VirtualKafkaCluster clusterProxy2b = MapperTestSupport.buildVirtualKafkaCluster(proxy2, "proxy2clusterb", serviceProxy2b);
            when(clusterList.getItems()).thenReturn(List.of(clusterProxy1, clusterProxy2a, clusterProxy2b));
            ReferencingSecondaryMapper<KafkaService> mapper = new ReferencingSecondaryMapper<>(context, KafkaService.class, SERVICE_TO_PROXY);

            // When / Then
            assertThat(mapper.toSecondaryResourceIDs(proxy1)).containsExactly(ResourceID.fromResource(serviceProxy1));
            assertThat(mapper.toSecondaryResourceIDs(proxy2))
                    .containsExactly(ResourceID.fromResource(serviceProxy2a), ResourceID.fromResource(serviceProxy2b));
        }

        @Test
        void proxyToKafkaServiceIgnoresDanglingKafkaService() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            MapperTestSupport.mockKafkaServiceListOperation(client);
            VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(proxy, "cluster", MapperTestSupport.buildKafkaService("dangle", 1L, 1L));
            when(clusterList.getItems()).thenReturn(List.of(cluster));
            ReferencingSecondaryMapper<KafkaService> mapper = new ReferencingSecondaryMapper<>(context, KafkaService.class, SERVICE_TO_PROXY);

            // When
            Set<ResourceID> secondaryResourceIDs = mapper.toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryResourceIDs).isEmpty();
        }
    }
}
