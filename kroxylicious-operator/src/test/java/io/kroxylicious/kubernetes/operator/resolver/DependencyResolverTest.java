/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.common.FilterRef;
import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRef;
import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.common.ProxyRef;
import io.kroxylicious.kubernetes.api.common.ProxyRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult.DanglingReference;

import edu.umd.cs.findbugs.annotations.NonNull;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mock.Strictness.LENIENT;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DependencyResolverTest {

    public static final String PROXY_NAME = "proxy";
    public static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata().build();
    @Mock(strictness = LENIENT)
    Context<KafkaProxy> mockProxyContext;
    private final DependencyResolver dependencyResolver = DependencyResolver.create();

    @BeforeEach
    void setup() {
        givenFiltersInContext();
        givenKafkaServicesInContext();
        givenIngressesInContext();
        givenVirtualKafkaClustersInContext();
        givenProxiesInContext();
    }

    @Test
    void resolveProxyRefsWhenNoEntitiesReferenceProxy() {
        // given nothing in context

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).isEmpty();
        assertThat(resolutionResult.clusterResolutionResults()).isEmpty();
        assertThat(resolutionResult.fullyResolvedClustersInNameOrder()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithManyReferencesDangling() {
        // given nothing in context

        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("missing")), "cluster", List.of(ingress("ingressRef")), getProxyRef(PROXY_NAME));

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactlyInAnyOrder(danglingReference(cluster, getProxyRef(PROXY_NAME)),
                danglingReference(cluster, getKafkaServiceRef("cluster")),
                danglingReference(cluster, ingress("ingressRef").getIngressRef()),
                danglingReference(cluster, filterRef("missing")));
    }

    @Test
    void resolveProxyRefsWithNullFiltersOnVirtualCluster() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(onlyResult.cluster())).isFalse();
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithNullFilters() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(clusterResolutionResult.cluster())).isFalse();
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithResolvedRefsFalseCondition() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long latestGeneration = 1L;
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME))
                .edit()
                .withNewMetadata().withGeneration(latestGeneration).endMetadata()
                .editStatus().addToConditions(resolvedRefsFalse(latestGeneration)).endStatus().build();
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(onlyResult.cluster())).isTrue();
    }

    @Test
    void shouldIgnoreStaleResolvedRefsFalseConditionOfResolvedCluster() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long latestGeneration = 2L;
        long staleGeneration = 1L;
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME))
                .edit()
                .withNewMetadata().withGeneration(latestGeneration).endMetadata()
                .editStatus().addToConditions(resolvedRefsFalse(staleGeneration)).endStatus().build();
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(onlyResult.cluster())).isFalse();
    }

    @Test
    void resolveProxyRefsWithStaleKafkaServiceStatus() {
        // given
        KafkaService kafkaService = kafkaService("cluster", 2L);
        givenKafkaServicesInContext(kafkaService);
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allResolvedReferents().allMatch(ResourcesUtil.isStatusStale())).isTrue();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.isStatusStale()).<LocalRef<?>> map(ResourcesUtil::toLocalRef)).containsExactly(
                ResourcesUtil.toLocalRef(kafkaService));
    }

    @Test
    void resolveProxyRefsWithStaleKafkaProxyIngressStatus() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 2L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allResolvedReferents().allMatch(ResourcesUtil.isStatusStale())).isFalse();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.isStatusStale())).containsExactly(ingress);
    }

    @Test
    void resolveProxyRefsWithStaleKafkaProtocolFilterStatus() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 2L, 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allResolvedReferents().allMatch(ResourcesUtil.isStatusStale())).isFalse();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.isStatusStale())).containsExactly(filter);
    }

    @Test
    void resolveProxyRefsWithStaleVirtualKafkaClusterStatus() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long oldGeneration = 1L;
        long generation = 2L;
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME))
                .edit().editMetadata().withGeneration(generation).endMetadata()
                .editStatus().withObservedGeneration(oldGeneration).endStatus()
                .build();
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        // no referents are stale, only the cluster
        assertThat(onlyResult.allResolvedReferents().noneMatch(ResourcesUtil.isStatusStale())).isTrue();
        assertThat(!ResourcesUtil.isStatusFresh(onlyResult.cluster())).isTrue();
    }

    @Test
    void resolveClusterRefsWithStaleVirtualKafkaClusterStatus() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long oldGeneration = 1L;
        long generation = 2L;
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME))
                .edit().editMetadata().withGeneration(generation).endMetadata()
                .editStatus().withObservedGeneration(oldGeneration).endStatus()
                .build();

        // when
        ClusterResolutionResult resolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(resolutionResult.cluster()).isEqualTo(cluster);
        assertThat(resolutionResult.allResolvedReferents().noneMatch(ResourcesUtil.isStatusStale())).isTrue();
        assertThat(!ResourcesUtil.isStatusFresh(resolutionResult.cluster())).isTrue();
    }

    @Test
    void resolveClusterRefsWhenClusterHasResolvedRefsFalse() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long latestGeneration = 1L;
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME))
                .edit().withNewMetadata().withGeneration(latestGeneration).endMetadata()
                .editStatus().addToConditions(resolvedRefsFalse(latestGeneration)).endStatus().build();
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then

        // we want to ensure that when we are reconciling a VirtualKafkaCluster than we do not consider the status of that
        // cluster. Ie if a previous reconciliation sets the cluster to ResolvedRefs: False, we do not want that status to
        // be considered when calculating a fresh status.
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(ResourcesUtil.hasFreshResolvedRefsFalseCondition(clusterResolutionResult.cluster())).isTrue();
        assertThat(clusterResolutionResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isFalse();
    }

    @Test
    void resolveProxyRefsWithFilterHavingResolvedRefsFalseCondition() {
        // given
        long latestGeneration = 1L;
        KafkaProtocolFilter filter = protocolFilter("filterName", latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration))
                .endStatus().build();
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).containsExactly(filter);
        String referentKind = HasMetadata.getKind(KafkaProtocolFilter.class);
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(filter);

        assertThat(onlyResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    @Test
    void shouldIgnoreStaleResolvedRefsFalseConditionOfResolvedFilter() {
        // given
        long latestGeneration = 2L;
        long staleGeneration = 1L;
        KafkaProtocolFilter filter = protocolFilter("filterName", latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(staleGeneration)).endStatus()
                .build();
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isEmpty();
    }

    @Test
    void resolveClusterRefsWithFilterHavingResolvedRefsFalseCondition() {
        // given
        long latestGeneration = 1L;
        KafkaProtocolFilter filter = protocolFilter("filterName", latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration))
                .endStatus().build();
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil.isStatusStale())).containsExactly(filter);
        String referentKind = HasMetadata.getKind(KafkaProtocolFilter.class);
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(
                        filter);
        assertThat(clusterResolutionResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    private static @NonNull Condition resolvedRefsFalse(long observedGeneration) {
        return new ConditionBuilder().withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withObservedGeneration(observedGeneration)
                .withLastTransitionTime(Instant.EPOCH)
                .withMessage("BadThings")
                .withReason("BadThings").build();
    }

    @Test
    void resolveProxyRefsWithServiceHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        long latestGeneration = 1L;
        KafkaService service = kafkaService("cluster", latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration)).endStatus().build();
        givenKafkaServicesInContext(service);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .containsExactly(service);
        String referentKind = HasMetadata.getKind(KafkaService.class);
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(service);
        assertThat(onlyResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    @Test
    void shouldIgnoreStaleResolvedRefsFalseConditionOfResolvedKafkaService() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        long latestGeneration = 2L;
        long staleGeneration = 1L;
        KafkaService service = kafkaService("cluster", latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(staleGeneration)).endStatus().build();
        givenKafkaServicesInContext(service);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .isEmpty();
    }

    @Test
    void resolveClusterRefsWithServiceHavingResolvedRefsFalseCondition() {
        // given
        long latestGeneration = 1L;
        KafkaProtocolFilter filter = protocolFilter("filterName", latestGeneration);
        givenFiltersInContext(filter);
        KafkaService service = kafkaService("cluster", 1L).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration)).endStatus().build();
        givenKafkaServicesInContext(service);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .containsExactly(service);
        String referentKind = HasMetadata.getKind(KafkaService.class);
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(service);
        assertThat(clusterResolutionResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    @Test
    void resolveProxyRefsWithIngressHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long latestGeneration = 1L;
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration))
                .endStatus().build();
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .containsExactly(ingress);
        String referentKind = HasMetadata.getKind(KafkaProxyIngress.class);
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(ingress);
        assertThat(onlyResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    @Test
    void shouldIgnoreStaleResolvedRefsFalseConditionOfResolvedIngress() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        long latestGeneration = 2L;
        long staleGeneration = 1L;
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, latestGeneration).edit().withNewStatus().addToConditions(resolvedRefsFalse(staleGeneration))
                .endStatus().build();
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .isEmpty();
    }

    @Test
    void resolveClusterRefsWithIngressHavingResolvedRefsFalseCondition() {
        // given
        long latestGeneration = 1L;
        KafkaProtocolFilter filter = protocolFilter("filterName", latestGeneration);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L).edit().withNewStatus().addToConditions(resolvedRefsFalse(latestGeneration)).endStatus().build();
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil::hasFreshResolvedRefsFalseCondition))
                .containsExactly(ingress);
        String referentKind = HasMetadata.getKind(KafkaProxyIngress.class);
        assertThat(clusterResolutionResult.allResolvedReferents().filter(ResourcesUtil.hasFreshResolvedRefsFalseCondition().and(ResourcesUtil.hasKind(referentKind))))
                .containsExactly(ingress);
        assertThat(clusterResolutionResult.allResolvedReferents().anyMatch(ResourcesUtil::hasFreshResolvedRefsFalseCondition)).isTrue();
    }

    @Test
    void resolveProxyRefsWithSingleDanglingFilterRef() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("another")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allDanglingReferences()).containsExactly(danglingReference(cluster, filterRef("another")));
    }

    @Test
    void resolveClusterRefsWithSingleDanglingFilterRef() {
        // given
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("another")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(danglingReference(cluster, filterRef("another")));
    }

    @Test
    void resolveProxyRefsWithSingleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allDanglingReferences()).isEmpty();
        assertThat(onlyResult.filterResolutionResults()).singleElement().satisfies(result -> {
            assertThat(result.referentResource()).isEqualTo(filter);
        });
    }

    @Test
    void resolveClusterRefsWithSingleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.allDanglingReferences()).isEmpty();
        assertThat(clusterResolutionResult.filterResolutionResults()).singleElement().satisfies(result -> {
            assertThat(result.referentResource()).isEqualTo(filter);
        });
    }

    @Test
    void resolveProxyRefsWithMultipleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        KafkaProtocolFilter filter2 = protocolFilter("filterName2", 1L);
        givenFiltersInContext(filter, filter2);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allDanglingReferences()).isEmpty();
        assertThat(onlyResult.filterResolutionResults().stream().map(ResolutionResult::referentResource)).containsExactly(filter, filter2);
    }

    @Test
    void resolveClusterRefsWithMultipleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        KafkaProtocolFilter filter2 = protocolFilter("filterName2", 1L);
        givenFiltersInContext(filter, filter2);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithSubsetOfFiltersReferenced() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allDanglingReferences()).containsExactly(danglingReference(cluster, filterRef("filterName2")));
    }

    @Test
    void resolveClusterRefsWithSubsetOfFiltersReferenced() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName", 1L);
        givenFiltersInContext(filter);
        givenKafkaServicesInContext(kafkaService("cluster", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(danglingReference(cluster, filterRef("filterName2")));
    }

    @Test
    void resolveProxyRefsWithDanglingIngressRef() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingressMissing")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allDanglingReferences()).containsExactly(danglingReference(cluster, ingress("ingressMissing").getIngressRef()));
    }

    @Test
    void resolveClusterRefsWithDanglingIngressRef() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingressMissing")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences())
                .containsExactly(danglingReference(cluster, ingress("ingressMissing").getIngressRef()));
    }

    @Test
    void resolveProxyRefsWithDanglingKafkaServiceRef() {
        // given
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "missing", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allDanglingReferences()).containsExactly(danglingReference(cluster, getKafkaServiceRef("missing")));
    }

    @Test
    void resolveClusterRefsWithDanglingKafkaServiceRef() {
        // given
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "missing", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(danglingReference(cluster, getKafkaServiceRef("missing")));
    }

    @Test
    void resolveProxyRefsWithSingleResolvedIngress() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithSingleResolvedIngress() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithMultipleResolvedIngresses() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        KafkaProxyIngress ingress2 = ingress("ingress2", PROXY_NAME, 1L);
        givenIngressesInContext(ingress, ingress2);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress"), ingress("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isTrue();
        assertThat(onlyResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithMultipleResolvedIngresses() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        KafkaProxyIngress ingress2 = ingress("ingress2", PROXY_NAME, 1L);
        givenIngressesInContext(ingress, ingress2);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress"), ingress("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.allDanglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithSubsetOfIngressesResolved() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress"), ingress("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.allReferentsFullyResolved()).isFalse();
        assertThat(onlyResult.allDanglingReferences()).containsExactly(danglingReference(cluster, ingress("ingress2").getIngressRef()));
    }

    @Test
    void resolveClusterRefsWithSubsetOfIngressesResolved() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress"), ingress("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(danglingReference(cluster, ingress("ingress2").getIngressRef()));
    }

    @Test
    void resolveClusterRefsWithIngressReferencingDifferentProxyThanCluster() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", "another-proxy", 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        LocalRef<?> proxyRef = getProxyRef("another-proxy");
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(new DanglingReference(ResourcesUtil.toLocalRef(ingress), proxyRef));
    }

    @Test
    void resolveClusterRefsWithDanglingProxyRef() {
        // given
        givenKafkaServicesInContext(kafkaService("clusterRef", 1L));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME, 1L);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingress("ingress")),
                getProxyRef("another-proxy"));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.allReferentsFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.allDanglingReferences()).containsExactly(danglingReference(cluster, getProxyRef("another-proxy")));
    }

    private @NonNull ClusterResolutionResult resolveClusterRefs(VirtualKafkaCluster cluster) {
        return dependencyResolver.resolveClusterRefs(cluster, mockProxyContext);
    }

    private @NonNull ProxyResolutionResult resolveProxyRefs(KafkaProxy proxy) {
        return dependencyResolver.resolveProxyRefs(proxy, mockProxyContext);
    }

    private static KafkaServiceRef getKafkaServiceRef(String name) {
        return new KafkaServiceRefBuilder().withName(name).build();
    }

    private static ProxyRef getProxyRef(String name) {
        return new ProxyRefBuilder().withName(name).build();
    }

    private Ingresses ingress(String name) {
        return new IngressesBuilder().editOrNewIngressRef().withName(name).endIngressRef().build();
    }

    private static KafkaProxyIngress ingress(String name, String proxyName, long generation) {
        return getKafkaProxyIngress(name, proxyName, generation, 1L);
    }

    private static KafkaProxyIngress getKafkaProxyIngress(String name, String proxyName, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withGeneration(generation)
                    .withName(name)
                .endMetadata()
                .withNewSpec()
                    .withNewProxyRef()
                        .withName(proxyName)
                    .endProxyRef()
                .endSpec()
                .withNewStatus()
                    .withObservedGeneration(observedGeneration)
                .endStatus()
                .build();
        // @formatter:on
    }

    private static ClusterResolutionResult assertSingleResult(ProxyResolutionResult resolutionResult, VirtualKafkaCluster cluster) {
        Set<ClusterResolutionResult> result = resolutionResult.clusterResolutionResults();
        assertThat(result).hasSize(1);
        ClusterResolutionResult onlyResult = result.stream().findFirst().orElseThrow();
        assertThat(onlyResult.cluster()).isEqualTo(cluster);
        return onlyResult;
    }

    private static KafkaService kafkaService(String clusterRef, long generation) {
        return kafkaService(clusterRef, generation, 1L);
    }

    private static KafkaService kafkaService(String clusterRef, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(clusterRef)
                    .withGeneration(generation)
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(observedGeneration)
                .endStatus()
                .build();
        // @formatter:on
    }

    private static FilterRef filterRef(String name) {
        return new FilterRefBuilder().withName(name).build();
    }

    private static KafkaProtocolFilter protocolFilter(String name, long generation) {
        return protocolFilter(name, generation, generation);
    }

    private static KafkaProtocolFilter protocolFilter(String name, long generation, long observedGeneration) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withApiVersion("filter.kroxylicious.io/v1alpha")
                .withKind("KafkaProtocolFilter")
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

    private static @NonNull DanglingReference danglingReference(VirtualKafkaCluster fromCluster, LocalRef<?> toRef) {
        return new DanglingReference(ResourcesUtil.toLocalRef(fromCluster), toRef);
    }

    private void givenFiltersInContext(KafkaProtocolFilter... resources) {
        givenSecondaryResourcesInContext(KafkaProtocolFilter.class, resources);
    }

    private void givenIngressesInContext(KafkaProxyIngress... ingresses) {
        givenSecondaryResourcesInContext(KafkaProxyIngress.class, ingresses);
    }

    private void givenVirtualKafkaClustersInContext(VirtualKafkaCluster... virtualKafkaClusters) {
        givenSecondaryResourcesInContext(VirtualKafkaCluster.class, virtualKafkaClusters);
    }

    private void givenProxiesInContext(KafkaProxy... kafkaProxies) {
        givenSecondaryResourcesInContext(KafkaProxy.class, kafkaProxies);
    }

    private void givenKafkaServicesInContext(KafkaService... kafkaServices) {
        givenSecondaryResourcesInContext(KafkaService.class, kafkaServices);
    }

    @SafeVarargs
    private <T> void givenSecondaryResourcesInContext(Class<T> type, T... resources) {
        when(mockProxyContext.getSecondaryResources(type)).thenReturn(Arrays.stream(resources).collect(Collectors.toSet()));
    }

    private static VirtualKafkaCluster virtualCluster(List<FilterRef> filterRefs, String clusterRef, List<Ingresses> ingresses, ProxyRef proxyRef) {
        return new VirtualKafkaClusterBuilder()
                .withNewSpec()
                .withIngresses(ingresses)
                .withNewTargetKafkaServiceRef().withName(clusterRef).endTargetKafkaServiceRef()
                .withProxyRef(proxyRef)
                .withFilterRefs(filterRefs)
                .endSpec()
                .build();
    }

}
