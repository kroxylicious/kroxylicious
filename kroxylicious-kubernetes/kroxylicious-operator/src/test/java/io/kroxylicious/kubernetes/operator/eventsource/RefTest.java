/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterSpec;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link Ref}'s factories end to end, through the {@link Relationship} they return, covering both
 * reference directions: {@link Ref#toPrimary}/{@link Ref#toPrimaryVia} (the secondary, perhaps transitively,
 * holds the pointer back to the primary &mdash; e.g. {@code KafkaProxyReconciler}) and
 * {@link Ref#toSecondary}/{@link Ref#toSecondaries} (the primary holds the pointer to its secondaries &mdash;
 * e.g. {@code VirtualKafkaClusterReconciler}).
 */
class RefTest {

    @Nested
    class ToPrimary {

        private static final Relationship<KafkaProxy, VirtualKafkaCluster> VKC_TO_PROXY = Ref.toPrimary(vkc -> vkc.getSpec().getProxyRef());

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
            Set<ResourceID> secondaryIds = VKC_TO_PROXY.primaryToSecondary(context, VirtualKafkaCluster.class).toSecondaryResourceIDs(proxy);

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
            Set<ResourceID> secondaryIds = VKC_TO_PROXY.primaryToSecondary(context, VirtualKafkaCluster.class).toSecondaryResourceIDs(proxy);

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
            Set<ResourceID> primaryIds = VKC_TO_PROXY.secondaryToPrimary(context).toPrimaryResourceIDs(cluster);

            // Then
            assertThat(primaryIds).containsExactlyInAnyOrder(ResourceID.fromResource(proxy1), ResourceID.fromResource(proxy2));
        }

        @Test
        void secondaryToPrimaryDoesNotFilterStaleStatus() {
            // the freshness guard lives in EventSourceBuilder, not in Ref; see EventSourceBuilderTest
            // Given
            KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
            EventSourceContext<KafkaProxy> context = MapperTestSupport.mockContextContaining(proxy);
            VirtualKafkaCluster staleCluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 2L, 1L).build();

            // When
            Set<ResourceID> primaryIds = VKC_TO_PROXY.secondaryToPrimary(context).toPrimaryResourceIDs(staleCluster);

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
            Set<ResourceID> primaryIds = VKC_TO_PROXY.secondaryToPrimary(context).toPrimaryResourceIDs(freshCluster);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(proxy));
        }
    }

    @Nested
    class ToPrimaryVia {

        private static final Relationship<KafkaProxy, VirtualKafkaCluster> VKC_TO_PROXY = Ref.toPrimary(vkc -> vkc.getSpec().getProxyRef());
        private static final Relationship<KafkaProxy, KafkaProtocolFilter> FILTER_TO_PROXY = Ref.toPrimaryVia(VirtualKafkaCluster.class,
                Ref.refersToAny(vkc -> vkc.getSpec().getFilterRefs()), VKC_TO_PROXY);
        private static final Relationship<KafkaProxy, KafkaService> SERVICE_TO_PROXY = Ref.toPrimaryVia(VirtualKafkaCluster.class,
                Ref.refersTo(vkc -> vkc.getSpec().getTargetKafkaServiceRef()), VKC_TO_PROXY);

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
        void throwsWhenIntermediateToPrimaryNotBuiltViaRef() {
            // Given
            Relationship<KafkaProxy, VirtualKafkaCluster> notBuiltViaRef = new Relationship<>() {
                @Override
                public PrimaryToSecondaryMapper<KafkaProxy> primaryToSecondary(EventSourceContext<KafkaProxy> context, Class<VirtualKafkaCluster> secondaryType) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public SecondaryToPrimaryMapper<VirtualKafkaCluster> secondaryToPrimary(EventSourceContext<KafkaProxy> context) {
                    throw new UnsupportedOperationException();
                }
            };
            BiPredicate<VirtualKafkaCluster, HasMetadata> intermediateRefersToSecondary = Ref.refersToAny(vkc -> vkc.getSpec().getFilterRefs());

            // When / Then
            assertThatThrownBy(() -> Ref.toPrimaryVia(VirtualKafkaCluster.class, intermediateRefersToSecondary, notBuiltViaRef))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class ToSecondary {

        private static final Relationship<VirtualKafkaCluster, KafkaService> VKC_TO_SERVICE = Ref
                .toSecondary(cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef()));

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
            VirtualKafkaCluster otherCluster = MapperTestSupport.buildVirtualKafkaCluster(MapperTestSupport.buildProxy("proxy"), "othercluster",
                    MapperTestSupport.buildKafkaService("otherservice", 1L, 1L));
            EventSourceContext<VirtualKafkaCluster> context = MapperTestSupport.mockContextContaining(referencingCluster, otherCluster);

            // When
            Set<ResourceID> primaryIds = VKC_TO_SERVICE.secondaryToPrimary(context).toPrimaryResourceIDs(service);

            // Then
            assertThat(primaryIds).containsExactly(ResourceID.fromResource(referencingCluster));
        }
    }

    @Nested
    class ToSecondaries {

        private static final Relationship<VirtualKafkaCluster, KafkaProtocolFilter> VKC_TO_FILTERS = Ref.toSecondaries(
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
    class RefersToPredicates {

        @Test
        void refersToIsNullSafeWhenRefMissing() {
            // Given
            VirtualKafkaCluster clusterWithoutServiceRef = new VirtualKafkaClusterBuilder()
                    .withNewSpec().withNewProxyRef().withName("test-proxy").endProxyRef().endSpec().build();
            var predicate = Ref.<VirtualKafkaCluster, KafkaService> refersTo(vkc -> vkc.getSpec().getTargetKafkaServiceRef());

            // When / Then
            assertThat(predicate.test(clusterWithoutServiceRef, MapperTestSupport.buildKafkaService("svc", 1L, 1L))).isFalse();
        }

        @Test
        void refersToAnyIsNullSafeWhenRefsMissing() {
            // Given
            VirtualKafkaCluster clusterWithoutFilterRefs = new VirtualKafkaClusterBuilder()
                    .withNewSpec().withNewProxyRef().withName("test-proxy").endProxyRef().endSpec().build();
            var predicate = Ref.<VirtualKafkaCluster, KafkaProtocolFilter> refersToAny(vkc -> vkc.getSpec().getFilterRefs());

            // When / Then
            assertThat(predicate.test(clusterWithoutFilterRefs, MapperTestSupport.buildFilter("filter", 1L, 1L))).isFalse();
        }
    }
}
