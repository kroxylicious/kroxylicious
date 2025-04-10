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
import io.kroxylicious.kubernetes.api.common.IngressRef;
import io.kroxylicious.kubernetes.api.common.IngressRefBuilder;
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
        givenClusterRefsInContext();
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
        assertThat(resolutionResult.ingresses()).isEmpty();
        assertThat(resolutionResult.fullyResolvedClustersInNameOrder()).isEmpty();
        assertThat(resolutionResult.filter(filterRef("c"))).isEmpty();
        assertThat(resolutionResult.filters()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithManyReferencesDangling() {
        // given nothing in context

        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("missing")), "cluster", List.of(ingressRef("ingressRef")), getProxyRef(PROXY_NAME));

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactlyInAnyOrder(danglingReference(cluster, getProxyRef(PROXY_NAME)),
                danglingReference(cluster, getKafkaServiceRef("cluster")),
                danglingReference(cluster, ingressRef("ingressRef")),
                danglingReference(cluster, filterRef("missing")));
    }

    @Test
    void resolveProxyRefsWithNullFiltersOnVirtualCluster() {
        // given
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.allClustersInNameOrder()).containsExactly(cluster);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithNullFilters() {
        // given
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(null, "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithFilterHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName").edit().withNewStatus().addToConditions(resolvedRefsFalse()).endStatus().build();
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse()).containsExactly(filterRef("filterName"));
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaProtocolFilter.class)))
                .containsExactly(filterRef("filterName"));
    }

    @Test
    void resolveClusterRefsWithFilterHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName").edit().withNewStatus().addToConditions(resolvedRefsFalse())
                .endStatus().build();
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse()).containsExactly(filterRef("filterName"));
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaProtocolFilter.class))).containsExactly(filterRef("filterName"));
    }

    private static @NonNull Condition resolvedRefsFalse() {
        return new ConditionBuilder().withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withObservedGeneration(1L)
                .withLastTransitionTime(Instant.EPOCH)
                .withMessage("BadThings")
                .withReason("BadThings").build();
    }

    @Test
    void resolveProxyRefsWithServiceHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        KafkaService service = kafkaService("cluster").edit().withNewStatus().addToConditions(resolvedRefsFalse()).endStatus().build();
        givenClusterRefsInContext(service);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse()).containsExactly(getKafkaServiceRef("cluster"));
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaService.class)))
                .containsExactly(getKafkaServiceRef("cluster"));
    }

    @Test
    void resolveClusterRefsWithServiceHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        KafkaService service = kafkaService("cluster").edit().withNewStatus().addToConditions(resolvedRefsFalse()).endStatus().build();
        givenClusterRefsInContext(service);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse()).containsExactly(getKafkaServiceRef("cluster"));
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaService.class))).containsExactly(getKafkaServiceRef("cluster"));
    }

    @Test
    void resolveProxyRefsWithIngressHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME).edit().withNewStatus().addToConditions(resolvedRefsFalse()).endStatus().build();
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(ingressRef("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse()).containsExactly(ingressRef("ingress"));
        assertThat(onlyResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaProxyIngress.class)))
                .containsExactly(ingressRef("ingress"));
    }

    @Test
    void resolveClusterRefsWithIngressHavingResolvedRefsFalseCondition() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME).edit().withNewStatus().addToConditions(resolvedRefsFalse()).endStatus().build();
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(ingressRef("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse()).containsExactly(ingressRef("ingress"));
        assertThat(clusterResolutionResult.findResourcesWithResolvedRefsFalse(HasMetadata.getKind(KafkaProxyIngress.class))).containsExactly(ingressRef("ingress"));
    }

    @Test
    void resolveProxyRefsWithSingleDanglingFilterRef() {
        // given
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("another")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.filter(filterRef("another"))).isEmpty();
        assertThat(resolutionResult.filters()).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.danglingReferences()).containsExactly(danglingReference(cluster, filterRef("another")));
    }

    @Test
    void resolveClusterRefsWithSingleDanglingFilterRef() {
        // given
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("another")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, filterRef("another")));
    }

    @Test
    void resolveProxyRefsWithSingleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filters()).containsExactly(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithSingleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName")), "cluster", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithMultipleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        KafkaProtocolFilter filter2 = protocolFilter("filterName2");
        givenFiltersInContext(filter, filter2);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filter(filterRef("filterName2"))).contains(filter2);
        assertThat(resolutionResult.filters()).containsExactlyInAnyOrder(filter, filter2);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithMultipleResolvedFilter() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        KafkaProtocolFilter filter2 = protocolFilter("filterName2");
        givenFiltersInContext(filter, filter2);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithSubsetOfFiltersReferenced() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.filter(filterRef("filterName"))).contains(filter);
        assertThat(resolutionResult.filter(filterRef("filterName2"))).isEmpty();
        assertThat(resolutionResult.filters()).containsExactlyInAnyOrder(filter);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.danglingReferences()).containsExactly(danglingReference(cluster, filterRef("filterName2")));
    }

    @Test
    void resolveClusterRefsWithSubsetOfFiltersReferenced() {
        // given
        KafkaProtocolFilter filter = protocolFilter("filterName");
        givenFiltersInContext(filter);
        givenClusterRefsInContext(kafkaService("cluster"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(filterRef("filterName"), filterRef("filterName2")), "cluster", List.of(),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, filterRef("filterName2")));
    }

    @Test
    void resolveProxyRefsWithDanglingIngressRef() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingressMissing")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.ingresses()).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.danglingReferences()).containsExactly(danglingReference(cluster, ingressRef("ingressMissing")));
    }

    @Test
    void resolveClusterRefsWithDanglingIngressRef() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingressMissing")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, ingressRef("ingressMissing")));
    }

    @Test
    void resolveProxyRefsWithDanglingKafkaServiceRef() {
        // given
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "missing", List.of(), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.ingresses()).isEmpty();
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.danglingReferences()).containsExactly(danglingReference(cluster, getKafkaServiceRef("missing")));
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
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, getKafkaServiceRef("missing")));
    }

    @Test
    void resolveProxyRefsWithSingleResolvedIngress() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.ingresses()).containsExactly(ingress);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithSingleResolvedIngress() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress")), getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithMultipleResolvedIngresses() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        KafkaProxyIngress ingress2 = ingress("ingress2", PROXY_NAME);
        givenIngressesInContext(ingress, ingress2);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.ingresses()).containsExactlyInAnyOrder(ingress, ingress2);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isTrue();
        assertThat(onlyResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveClusterRefsWithMultipleResolvedIngresses() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        KafkaProxyIngress ingress2 = ingress("ingress2", PROXY_NAME);
        givenIngressesInContext(ingress, ingress2);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isTrue();
        assertThat(clusterResolutionResult.danglingReferences()).isEmpty();
    }

    @Test
    void resolveProxyRefsWithSubsetOfIngressesResolved() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);

        // when
        ProxyResolutionResult resolutionResult = resolveProxyRefs(PROXY);

        // then
        assertThat(resolutionResult.ingresses()).containsExactlyInAnyOrder(ingress);
        ClusterResolutionResult onlyResult = assertSingleResult(resolutionResult, cluster);
        assertThat(onlyResult.isFullyResolved()).isFalse();
        assertThat(onlyResult.danglingReferences()).containsExactly(danglingReference(cluster, ingressRef("ingress2")));
    }

    @Test
    void resolveClusterRefsWithSubsetOfIngressesResolved() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress"), ingressRef("ingress2")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, ingressRef("ingress2")));
    }

    @Test
    void resolveClusterRefsWithIngressReferencingDifferentProxyThanCluster() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", "another-proxy");
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress")),
                getProxyRef(PROXY_NAME));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        LocalRef<?> proxyRef = getProxyRef("another-proxy");
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(new DanglingReference(ResourcesUtil.toLocalRef(ingress), proxyRef));
    }

    @Test
    void resolveClusterRefsWithDanglingProxyRef() {
        // given
        givenClusterRefsInContext(kafkaService("clusterRef"));
        KafkaProxyIngress ingress = ingress("ingress", PROXY_NAME);
        givenIngressesInContext(ingress);
        VirtualKafkaCluster cluster = virtualCluster(List.of(), "clusterRef", List.of(ingressRef("ingress")),
                getProxyRef("another-proxy"));
        givenVirtualKafkaClustersInContext(cluster);
        givenProxiesInContext(PROXY);

        // when
        ClusterResolutionResult clusterResolutionResult = resolveClusterRefs(cluster);

        // then
        assertThat(clusterResolutionResult.isFullyResolved()).isFalse();
        assertThat(clusterResolutionResult.danglingReferences()).containsExactly(danglingReference(cluster, getProxyRef("another-proxy")));
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

    private IngressRef ingressRef(String name) {
        return new IngressRefBuilder().withName(name).build();
    }

    private static KafkaProxyIngress ingress(String name, String proxyName) {
        return new KafkaProxyIngressBuilder().withNewMetadata().withName(name).endMetadata()
                .withNewSpec().withNewProxyRef().withName(proxyName).endProxyRef().endSpec().build();
    }

    private static ClusterResolutionResult assertSingleResult(ProxyResolutionResult resolutionResult, VirtualKafkaCluster cluster) {
        Set<ClusterResolutionResult> result = resolutionResult.clusterResolutionResults();
        assertThat(result).hasSize(1);
        ClusterResolutionResult onlyResult = result.stream().findFirst().orElseThrow();
        assertThat(onlyResult.cluster()).isEqualTo(cluster);
        return onlyResult;
    }

    private static KafkaService kafkaService(String clusterRef) {
        return new KafkaServiceBuilder().withNewMetadata().withName(clusterRef).endMetadata().build();
    }

    private static FilterRef filterRef(String name) {
        return new FilterRefBuilder().withName(name).build();
    }

    private static KafkaProtocolFilter protocolFilter(String name) {
        return new KafkaProtocolFilterBuilder()
                .withApiVersion("filter.kroxylicious.io/v1alpha")
                .withKind("KafkaProtocolFilter")
                .withNewMetadata().withName(name)
                .endMetadata()
                .build();
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

    private void givenClusterRefsInContext(KafkaService... clusterRefs) {
        givenSecondaryResourcesInContext(KafkaService.class, clusterRefs);
    }

    @SafeVarargs
    private <T> void givenSecondaryResourcesInContext(Class<T> type, T... resources) {
        when(mockProxyContext.getSecondaryResources(type)).thenReturn(Arrays.stream(resources).collect(Collectors.toSet()));
    }

    private static VirtualKafkaCluster virtualCluster(List<FilterRef> filterRefs, String clusterRef, List<IngressRef> ingressRefs, ProxyRef proxyRef) {
        return new VirtualKafkaClusterBuilder()
                .withNewSpec()
                .withIngressRefs(ingressRefs)
                .withNewTargetKafkaServiceRef().withName(clusterRef).endTargetKafkaServiceRef()
                .withProxyRef(proxyRef)
                .withFilterRefs(filterRefs)
                .endSpec()
                .build();
    }

}
