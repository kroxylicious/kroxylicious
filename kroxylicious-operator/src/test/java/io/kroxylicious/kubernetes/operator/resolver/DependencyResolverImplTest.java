/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.GenericKubernetesResourceBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaClusterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Filters;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.FiltersBuilder;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult.UnresolvedDependency;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DependencyResolverImplTest {

    @Mock(strictness = LENIENT)
    Context<KafkaProxy> mockContext;

    @Mock(strictness = LENIENT)
    UnresolvedDependencyReporter unresolvedDependencyReporter;

    @Test
    void testNoDependencies() {
        givenFiltersInContext();
        givenClusterRefsInContext();
        givenIngressesInContext();
        givenVirtualKafkaClustersInContext();

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).isEmpty();
        assertThat(resolutionResult.clusterResults()).isEmpty();
        assertThat(resolutionResult.ingresses()).isEmpty();
        assertThat(resolutionResult.fullyResolvedClustersInNameOrder()).isEmpty();
        assertThat(resolutionResult.filter(filterRef("a", "b", "c"))).isEmpty();
        assertThat(resolutionResult.filters()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testNullFiltersOnVirtualClusterTolerated() {
        givenFiltersInContext();
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testSingleFilterUnreferenced() {
        GenericKubernetesResource filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testSingleFilterUnreferencedBecauseOfKindMismatch() {
        GenericKubernetesResource filter = protocolFilter("filterName", "kroxylicious.io", "Kind1");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName", "kroxylicious.io", "Kind2")), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName", "kroxylicious.io", "Kind1"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.FILTER, "filterName"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testSingleFilterUnreferencedBecauseOfGroupMismatch() {
        GenericKubernetesResource filter = protocolFilter("filterName", "kroxylicious.io", "Kind");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName", "banana.io", "Kind")), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName", "kroxylicious.io", "Kind"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.FILTER, "filterName"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testSingleFilterReferenced() {
        GenericKubernetesResource filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testMultipleFiltersReferenced() {
        GenericKubernetesResource filter = protocolFilter("filterName");
        GenericKubernetesResource filter2 = protocolFilter("filterName2");
        givenFiltersInContext(filter, filter2);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filter(filterRef("filterName2"))).contains(filter2);
        assertThat(resolutionResult.filters()).containsExactlyInAnyOrder(filter, filter2);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testSubsetOfFiltersReferenced() {
        GenericKubernetesResource filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("cluster"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filter(filterRef("filterName2"))).isEmpty();
        assertThat(resolutionResult.filters()).containsExactlyInAnyOrder(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.FILTER, "filterName2"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testUnresolvedFilter() {
        GenericKubernetesResource filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaClusterRef("clusterRef"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("other")), "clusterRef", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.filter(filterRef("other"))).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.FILTER, "other"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testUnresolvedIngress() {
        givenFiltersInContext();
        givenClusterRefsInContext(kafkaClusterRef("clusterRef"));
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingressMissing")));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.ingresses()).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.KAFKA_PROXY_INGRESS, "ingressMissing"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testUnresolvedKafkaClusterRef() {
        givenFiltersInContext();
        givenClusterRefsInContext();
        givenIngressesInContext();
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "missing", List.of());
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.ingresses()).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.KAFKA_CLUSTER_REF, "missing"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    @Test
    void testSingleResolvedIngress() {
        givenFiltersInContext();
        givenClusterRefsInContext(kafkaClusterRef("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress");
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress")));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.ingresses()).containsExactly(ingress);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testMultipleResolvedIngresses() {
        givenFiltersInContext();
        givenClusterRefsInContext(kafkaClusterRef("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress");
        KafkaProxyIngress ingress2 = ingress("ingress2");
        givenIngressesInContext(ingress, ingress2);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.ingresses()).containsExactlyInAnyOrder(ingress, ingress2);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.unresolvedDependencySet()).isEmpty();
        verifyNoInteractions(unresolvedDependencyReporter);
    }

    @Test
    void testSubsetOfIngressesResolved() {
        givenFiltersInContext();
        givenClusterRefsInContext(kafkaClusterRef("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress");
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ResolutionResult resolutionResult = DependencyResolverImpl.create().deepResolve(mockContext, unresolvedDependencyReporter);

        // then
        assertThat(resolutionResult.ingresses()).containsExactlyInAnyOrder(ingress);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.unresolvedDependencySet()).containsExactly(new UnresolvedDependency(Dependency.KAFKA_PROXY_INGRESS, "ingress2"));
        verify(unresolvedDependencyReporter).reportUnresolvedDependencies(cluster, onlyResult.unresolvedDependencySet());
    }

    private IngressRef ingressRef(String name) {
        return new IngressRefBuilder().withName(name).build();
    }

    private static KafkaProxyIngress ingress(String name) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(name).endMetadata().build();
    }

    private static ClusterResolutionResult assertSingleResult(ResolutionResult resolutionResult, VirtualKafkaCluster cluster) {
        Collection<ClusterResolutionResult> result = resolutionResult.clusterResults();
        assertThat(result).hasSize(1);
        ClusterResolutionResult onlyResult = result.stream().findFirst().orElseThrow();
        assertThat(onlyResult.cluster()).isEqualTo(cluster);
        return onlyResult;
    }

    private static KafkaClusterRef kafkaClusterRef(String clusterRef) {
        return new KafkaClusterRefBuilder().withNewMetadata().withName(clusterRef).endMetadata().build();
    }

    private static Filters filterRef(String name) {
        return filterRef(name, "kroxylicious.io", "KafkaProtocolFilter");
    }

    private static Filters filterRef(String name, String group, String kind) {
        return new FiltersBuilder().withName(name).withGroup(group).withKind(kind).build();
    }

    private static GenericKubernetesResource protocolFilter(String name) {
        // should we build up a typed KafkaProtocolFilter and convert it to a generic resource somehow?
        return protocolFilter(name, "kroxylicious.io", "KafkaProtocolFilter");
    }

    private static GenericKubernetesResource protocolFilter(String name, String groupId, String kind) {
        return new GenericKubernetesResourceBuilder()
                .withApiVersion(groupId + "/v1alpha")
                .withKind(kind)
                .withNewMetadata().withName(name)
                .endMetadata().build();
    }

    private void givenFiltersInContext(GenericKubernetesResource... resources) {
        givenSecondaryResourcesInContext(GenericKubernetesResource.class, resources);
    }

    private void givenIngressesInContext(KafkaProxyIngress... ingresses) {
        givenSecondaryResourcesInContext(KafkaProxyIngress.class, ingresses);
    }

    private void givenVirtualKafkaClustersInContext(VirtualKafkaCluster... virtualKafkaClusters) {
        givenSecondaryResourcesInContext(VirtualKafkaCluster.class, virtualKafkaClusters);
    }

    private void givenClusterRefsInContext(KafkaClusterRef... clusterRefs) {
        givenSecondaryResourcesInContext(KafkaClusterRef.class, clusterRefs);
    }

    private <T> OngoingStubbing<Set<T>> givenSecondaryResourcesInContext(Class<T> type, T... resources) {
        return when(mockContext.getSecondaryResources(type)).thenReturn(Arrays.stream(resources).collect(Collectors.toSet()));
    }

    private static VirtualKafkaCluster virtualCluster(List<Filters> filterRefs, String clusterRef, List<IngressRef> ingressRefs) {
        return new VirtualKafkaClusterBuilder()
                .withNewSpec()
                .withIngressRefs(ingressRefs)
                .withNewTargetCluster().withNewClusterRef().withName(clusterRef).endClusterRef().endTargetCluster()
                .withFilters(filterRefs)
                .endSpec()
                .build();
    }

}
