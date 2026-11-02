/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link ReferenceDependency} end to end, through the {@link Dependency} they
 * produce, covering every hop shape a reconciler needs:
 * <ul>
 *     <li>a single hop, either direction ({@link ReferenceDependency#dependsOn} or {@link ReferenceDependency#dependsOnReferrer})</li>
 *     <li>a pure forward chain of hops (every hop owned by the near-to-far side)</li>
 *     <li>a chain that changes direction mid-path at a hub resource, via {@link ReferenceDependency#dependsOnReferrer}</li>
 *     <li>a pure backward chain (every hop owned by the far-to-near side) &mdash; the shape {@code Ref} couldn't
 *     express, motivating this DSL</li>
 *     <li>the event source name derived from a path's shape ({@link ReferenceDependency#eventSourceName()})</li>
 * </ul>
 */
class DependenciesTest {

    @Nested
    class SingleHopOwnedByPrimary {

        private static final Dependency<VirtualKafkaCluster, KafkaService> VKC_TO_SERVICE = ReferenceDependency.each(VirtualKafkaCluster.class)
                .dependsOn(KafkaService.class, "kafkaService", cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef()));

        @Test
        void primaryToSecondaryComputesResourceIdDirectlyWithoutScanning() {
            // Given
            EventSourceContext<VirtualKafkaCluster> context = mock();
            KafkaService service = MapperTestSupport.buildKafkaService("service", 1L, 1L);
            VirtualKafkaCluster cluster = MapperTestSupport.buildVirtualKafkaCluster(MapperTestSupport.buildProxy("proxy"), "cluster", service);

            // When
            Set<ResourceID> secondaryIds = VKC_TO_SERVICE.primaryToSecondary(context, KafkaService.class).toSecondaryResourceIDs(cluster);

            // Then
            assertThat(secondaryIds).containsExactly(new ResourceID("service", null));
        }

        @Test
        void secondaryToPrimaryFindsReferencingPrimaryViaCache() {
            // Given
            KafkaService service = MapperTestSupport.buildKafkaService("service", 1L, 1L);
            VirtualKafkaCluster referencingCluster = MapperTestSupport.buildVirtualKafkaCluster(MapperTestSupport.buildProxy("proxy"), "cluster", service);
            VirtualKafkaCluster otherCluster = MapperTestSupport
                    .buildVirtualKafkaCluster(MapperTestSupport.buildProxy("proxy"), "othercluster",
                            MapperTestSupport.buildKafkaService("otherservice", 1L, 1L));
            EventSourceContext<VirtualKafkaCluster> context = MapperTestSupport.mockContextContaining(referencingCluster, otherCluster);

            // When
            Set<ResourceID> primaryIds = VKC_TO_SERVICE.secondaryToPrimary(context).toPrimaryResourceIDs(service);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(referencingCluster));
        }
    }

    @Nested
    class SingleHopOwnedByPrimaryCollection {

        private static final Dependency<VirtualKafkaCluster, KafkaProtocolFilter> VKC_TO_FILTERS = ReferenceDependency.each(VirtualKafkaCluster.class)
                .dependsOnEach(KafkaProtocolFilter.class, "filters",
                        cluster -> Optional.ofNullable(cluster.getSpec()).map(VirtualKafkaClusterSpec::getFilterRefs).orElse(List.of()));

        @Test
        void primaryToSecondaryComputesResourceIdsDirectlyForEachRef() {
            // Given
            EventSourceContext<VirtualKafkaCluster> context = mock();
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("proxy"), "cluster", 1L, 1L)
                    .editOrNewSpec().addNewFilterRef().withName("filter1").endFilterRef().addNewFilterRef().withName("filter2").endFilterRef().endSpec().build();

            // When
            Set<ResourceID> secondaryIds = VKC_TO_FILTERS.primaryToSecondary(context, KafkaProtocolFilter.class).toSecondaryResourceIDs(cluster);

            // Then
            assertThat(secondaryIds).containsExactlyInAnyOrder(new ResourceID("filter1", null), new ResourceID("filter2", null));
        }

        @Test
        void primaryToSecondaryIsEmptyWhenNoRefs() {
            // Given
            EventSourceContext<VirtualKafkaCluster> context = mock();
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("proxy"), "cluster", 1L, 1L).build();

            // When
            Set<ResourceID> secondaryIds = VKC_TO_FILTERS.primaryToSecondary(context, KafkaProtocolFilter.class).toSecondaryResourceIDs(cluster);

            // Then
            assertThat(secondaryIds).isEmpty();
        }

        @Test
        void secondaryToPrimaryFindsReferencingPrimaryViaCache() {
            // Given
            KafkaProtocolFilter filter = MapperTestSupport.buildFilter("filter", 1L, 1L);
            VirtualKafkaCluster referencingCluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("proxy"), "cluster", 1L, 1L)
                    .editOrNewSpec().addNewFilterRef().withName("filter").endFilterRef().endSpec().build();
            VirtualKafkaCluster otherCluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(MapperTestSupport.buildProxy("proxy"), "othercluster", 1L, 1L).build();
            EventSourceContext<VirtualKafkaCluster> context = MapperTestSupport.mockContextContaining(referencingCluster, otherCluster);

            // When
            Set<ResourceID> primaryIds = VKC_TO_FILTERS.secondaryToPrimary(context).toPrimaryResourceIDs(filter);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(referencingCluster));
        }
    }

    @Nested
    class SingleHopOwnedBySecondary {

        private static final Dependency<KafkaProxy, VirtualKafkaCluster> PROXY_TO_VKC = ReferenceDependency.each(KafkaProxy.class)
                .dependsOnReferrer(VirtualKafkaCluster.class, "proxy", vkc -> Optional.ofNullable(vkc.getSpec().getProxyRef()));

        @Test
        void primaryToSecondaryIsNullSafeWhenSecondaryHasNoRef() {
            // Given
            VirtualKafkaCluster clusterWithoutProxyRef = new VirtualKafkaClusterBuilder().withNewSpec().endSpec().build();
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            when(clusterList.getItems()).thenReturn(List.of(clusterWithoutProxyRef));
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");

            // When
            Set<ResourceID> secondaryIds = PROXY_TO_VKC.primaryToSecondary(context, VirtualKafkaCluster.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).isEmpty();
        }

        @Test
        void primaryToSecondaryReturnsReferencingCluster() {
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

            // When
            Set<ResourceID> secondaryIds = PROXY_TO_VKC.primaryToSecondary(context, VirtualKafkaCluster.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).containsExactly(ResourceID.fromResource(cluster));
        }

        @Test
        void secondaryToPrimaryMapsSecondaryToAllPrimariesInNamespace() {
            // Given
            KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
            KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
            EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy1, proxy2);
            VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy1, "cluster", 1L, 1L).build();

            // When
            Set<ResourceID> primaryIds = PROXY_TO_VKC.secondaryToPrimary(context).toPrimaryResourceIDs(cluster);

            // Then
            assertThat(primaryIds).containsExactlyInAnyOrder(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
        }

        @Test
        void secondaryToPrimaryDoesNotFilterStaleStatus() {
            // the freshness guard lives in EventSourceBuilder, not here; see EventSourceBuilderTest
            // Given
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy);
            VirtualKafkaCluster staleCluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 2L, 1L).build();

            // When
            Set<ResourceID> primaryIds = PROXY_TO_VKC.secondaryToPrimary(context).toPrimaryResourceIDs(staleCluster);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(proxy));
        }

        @Test
        void secondaryToPrimaryReadsFromPrimaryCacheWhenApiServerUnavailable() {
            // Given
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            EventSourceContext<KafkaProxy> context = mock();
            MapperTestSupport.stubFailingListOperationClient(context, KafkaProxy.class);
            MapperTestSupport.stubPrimaryCache(context, proxy);
            VirtualKafkaCluster freshCluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();

            // When
            Set<ResourceID> primaryIds = PROXY_TO_VKC.secondaryToPrimary(context).toPrimaryResourceIDs(freshCluster);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(proxy));
        }
    }

    /**
     * The hub shape: {@code VirtualKafkaCluster} owns a ref to the proxy, and separately owns refs to
     * filters/services. Walking from {@code KafkaProxy}, {@link ReferenceDependency#dependsOnReferrer} reaches the hub
     * ({@code VirtualKafkaCluster}) in one hop; {@link ReferenceDependency#dependsOn}/{@link ReferenceDependency#dependsOnEach} then
     * continue forward from there to each of its other referents &mdash; no path splicing or whole-path
     * reversal needed.
     */
    @Nested
    class HubReachedByBackwardHop {

        private static final ReferenceDependency<KafkaProxy, VirtualKafkaCluster> PROXY_TO_VKC = ReferenceDependency.each(KafkaProxy.class)
                .dependsOnReferrer(VirtualKafkaCluster.class, "proxy", vkc -> Optional.ofNullable(vkc.getSpec().getProxyRef()));
        private static final Dependency<KafkaProxy, KafkaProtocolFilter> FILTER_TO_PROXY = PROXY_TO_VKC
                .dependsOnEach(KafkaProtocolFilter.class, "filters", vkc -> vkc.getSpec().getFilterRefs());
        private static final Dependency<KafkaProxy, KafkaService> SERVICE_TO_PROXY = PROXY_TO_VKC
                .dependsOn(KafkaService.class, "kafkaService", vkc -> Optional.ofNullable(vkc.getSpec().getTargetKafkaServiceRef()));

        @Test
        void primaryToSecondaryIsNullSafeWhenIntermediateHasNoRelevantRef() {
            // Given
            EventSourceContext<KafkaProxy> context = mock();
            KubernetesClient client = mock();
            when(context.getClient()).thenReturn(client);
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            KubernetesResourceList<VirtualKafkaCluster> clusterList = MapperTestSupport.mockVirtualKafkaClusterListOperation(client);
            KubernetesResourceList<KafkaService> serviceList = MapperTestSupport.mockKafkaServiceListOperation(client);
            when(serviceList.getItems()).thenReturn(List.of());
            VirtualKafkaCluster clusterWithoutServiceRef = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
            when(clusterList.getItems()).thenReturn(List.of(clusterWithoutServiceRef));

            // When
            Set<ResourceID> secondaryIds = SERVICE_TO_PROXY.primaryToSecondary(context, KafkaService.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).isEmpty();
        }

        @Test
        void primaryToSecondaryReturnsReferencedExistingFilter() {
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

            // When
            Set<ResourceID> secondaryIds = FILTER_TO_PROXY.primaryToSecondary(context, KafkaProtocolFilter.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).containsExactly(ResourceID.fromResource(filter));
        }

        @Test
        void primaryToSecondaryReturnsEmptyForClusterWithoutFilters() {
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

            // When
            Set<ResourceID> secondaryIds = FILTER_TO_PROXY.primaryToSecondary(context, KafkaProtocolFilter.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).isEmpty();
        }

        @Test
        void primaryToSecondaryReturnsReferencedService() {
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

            // When
            Set<ResourceID> secondaryIds = SERVICE_TO_PROXY.primaryToSecondary(context, KafkaService.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).containsExactly(ResourceID.fromResource(kafkaService));
        }

        @Test
        void primaryToSecondaryDistinguishesByProxy() {
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
            PrimaryToSecondaryMapper<KafkaProxy> mapper = SERVICE_TO_PROXY.primaryToSecondary(context, KafkaService.class);

            // When / Then
            assertThat(mapper.toSecondaryResourceIDs(proxy1)).containsExactly(ResourceID.fromResource(serviceProxy1));
            assertThat(mapper.toSecondaryResourceIDs(proxy2)).containsExactly(ResourceID.fromResource(serviceProxy2));
        }

        @Test
        void primaryToSecondaryIgnoresDanglingFilterRef() {
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

            // When
            Set<ResourceID> secondaryIds = FILTER_TO_PROXY.primaryToSecondary(context, KafkaProtocolFilter.class).toSecondaryResourceIDs(proxy);

            // Then
            assertThat(secondaryIds).isEmpty();
        }

        @Test
        void secondaryToPrimaryFallsBackToAllPrimariesInNamespace() {
            // The proxy->VirtualKafkaCluster hop was traversed backward (VirtualKafkaCluster owns the ref, not
            // the proxy), so this isn't a pure forward chain: changing a filter reconciles every known proxy in
            // its namespace, not just the ones currently referencing it.
            // Given
            KafkaProxy proxy1 = MapperTestSupport.buildProxy("proxy1");
            KafkaProxy proxy2 = MapperTestSupport.buildProxy("proxy2");
            EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy1, proxy2);
            KafkaProtocolFilter filter = MapperTestSupport.buildFilter("filter", 1L, 1L);

            // When
            Set<ResourceID> primaryIds = FILTER_TO_PROXY.secondaryToPrimary(context).toPrimaryResourceIDs(filter);

            // Then
            assertThat(primaryIds).containsExactlyInAnyOrder(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
        }
    }

    @Nested
    class EventSourceNameDerivation {

        @Test
        void singleForwardHopNamesAfterStartTypeAndLabel() {
            // Given
            ReferenceDependency<VirtualKafkaCluster, KafkaProxy> referenceDependency = ReferenceDependency.each(VirtualKafkaCluster.class)
                    .dependsOn(KafkaProxy.class, "proxy", vkc -> Optional.ofNullable(vkc.getSpec().getProxyRef()));

            // When
            String name = referenceDependency.eventSourceName();

            // Then
            assertThat(name).isEqualTo("VirtualKafkaCluster->proxy");
        }

        @Test
        void backwardHopThenForwardHopMixDirectionArrows() {
            // Given
            ReferenceDependency<KafkaProxy, KafkaProtocolFilter> referenceDependency = ReferenceDependency.each(KafkaProxy.class)
                    .dependsOnReferrer(VirtualKafkaCluster.class, "proxy", vkc -> Optional.ofNullable(vkc.getSpec().getProxyRef()))
                    .dependsOnEach(KafkaProtocolFilter.class, "filters", vkc -> vkc.getSpec().getFilterRefs());

            // When
            String name = referenceDependency.eventSourceName();

            // Then
            assertThat(name).isEqualTo("KafkaProxy<-proxy->filters");
        }

        @Test
        void eventSourceBuilderDefaultsNameToTheDerivedEventSourceName() {
            // Given
            EventSourceContext<VirtualKafkaCluster> context = mock();
            when(context.getPrimaryResourceClass()).thenReturn(VirtualKafkaCluster.class);
            ReferenceDependency<VirtualKafkaCluster, KafkaProxy> referenceDependency = ReferenceDependency.each(VirtualKafkaCluster.class)
                    .dependsOn(KafkaProxy.class, "proxy", vkc -> Optional.ofNullable(vkc.getSpec().getProxyRef()));

            // When
            EventSource<KafkaProxy, VirtualKafkaCluster> eventSource = referenceDependency.eventSourceBuilder(context).build();

            // Then
            assertThat(eventSource).isNotNull();
        }
    }

}
