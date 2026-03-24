/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.api.model.ObjectMetaBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.fabric8.openshift.api.model.RouteIngress;
import io.fabric8.openshift.api.model.RouteIngressBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.operator.Annotations;

import edu.umd.cs.findbugs.annotations.NonNull;

import static io.kroxylicious.kubernetes.operator.model.RouteHostDetails.RouteFor.BOOTSTRAP;
import static io.kroxylicious.kubernetes.operator.model.RouteHostDetails.RouteFor.NODE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouteHostDetailsTest {

    private static final String NAMESPACE = "test-namespace";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String PROXY_NAME = "my-proxy";
    private static final String INGRESS_NAME = "my-ingress";
    private static final String HOST_WITHOUT_SUBDOMAIN = "apps-crc.testing";
    private static final String BOOTSTRAP_HOST_WITH_SUBDOMAIN = CLUSTER_NAME + "-bootstrap." + HOST_WITHOUT_SUBDOMAIN;
    private static final String NODE_HOST_WITH_SUBDOMAIN = CLUSTER_NAME + "-1." + HOST_WITHOUT_SUBDOMAIN;
    private static final String UNRESOLVED_HOST_BOOTSTRAP_SERVERS = "one-bootstrap.$(unresolvedRouteHost):9291";

    @Mock
    Context<KafkaProxy> context;

    @Test
    void routeForValuesReturnLowercaseToString() {
        assertThat(BOOTSTRAP).hasToString("bootstrap");
        assertThat(NODE).hasToString("node");
    }

    @Test
    void findFirstRouteHostWithoutSubdomainReturnsEmptyWhenListIsEmpty() {
        // when
        Optional<String> result = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void findFirstRouteHostWithoutSubdomainReturnsEmptyWhenParameterIsNotMatching() {
        // given
        RouteHostDetails differentRouteForDetails = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, INGRESS_NAME, NODE, HOST_WITHOUT_SUBDOMAIN);
        RouteHostDetails differentNamespaceDetails = new RouteHostDetails(
                "random-namespace", CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP, HOST_WITHOUT_SUBDOMAIN);
        RouteHostDetails differentClusterNameDetails = new RouteHostDetails(
                NAMESPACE, "random-cluster", INGRESS_NAME, BOOTSTRAP, HOST_WITHOUT_SUBDOMAIN);
        RouteHostDetails differentIngressNameDetails = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, "other-ingress", BOOTSTRAP, HOST_WITHOUT_SUBDOMAIN);

        // when
        Optional<String> differentRouteForResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(differentRouteForDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);
        Optional<String> differentNamespaceResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(differentNamespaceDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);
        Optional<String> differentClusterNameResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(differentClusterNameDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);
        Optional<String> differentIngressNameResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(differentIngressNameDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);

        // then
        assertThat(differentRouteForResult).isEmpty();
        assertThat(differentNamespaceResult).isEmpty();
        assertThat(differentClusterNameResult).isEmpty();
        assertThat(differentIngressNameResult).isEmpty();
    }

    @Test
    void findFirstRouteHostWithoutSubdomainReturnsFirstMatchWhenMultipleAreMatching() {
        // given
        String firstHostWithoutSubdomain = "first.domain.com";

        RouteHostDetails first = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP, firstHostWithoutSubdomain);
        RouteHostDetails second = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP, "second.domain.com");

        // when
        Optional<String> result = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(first, second), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);

        // then
        assertThat(result).contains(firstHostWithoutSubdomain);
    }

    @Test
    void findFirstRouteHostWithoutSubdomainReturnsMatchWhenRouteForIsMatching() {
        // given
        RouteHostDetails bootstrapDetails = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP, HOST_WITHOUT_SUBDOMAIN);
        RouteHostDetails nodeDetails = new RouteHostDetails(
                NAMESPACE, CLUSTER_NAME, INGRESS_NAME, NODE, HOST_WITHOUT_SUBDOMAIN);

        // when
        Optional<String> bootstrapResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(bootstrapDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, BOOTSTRAP);
        Optional<String> nodeResult = RouteHostDetails.findFirstRouteHostWithoutSubdomainFromList(
                List.of(nodeDetails), NAMESPACE, CLUSTER_NAME, INGRESS_NAME, NODE);

        // then
        assertThat(bootstrapResult).contains(HOST_WITHOUT_SUBDOMAIN);
        assertThat(nodeResult).contains(HOST_WITHOUT_SUBDOMAIN);
    }

    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenNoRouteSecondaryResourcesFound() {
        // given
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of());

        // when
        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenRouteHasNoBootstrapServersAnnotation() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(BOOTSTRAP.toString(), false);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenRouteHasEmptyIngress() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(BOOTSTRAP.toString(), true);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(List.of())
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenRouteForLabelIsMissing() {
        // given
        ObjectMetaBuilder metaBuilder = new ObjectMetaBuilder()
                .withNamespace(NAMESPACE)
                .addToLabels(commonLabels());

        Annotations.annotateWithBootstrapServers(metaBuilder,
                Set.of(new Annotations.ClusterIngressBootstrapServers(CLUSTER_NAME, INGRESS_NAME, UNRESOLVED_HOST_BOOTSTRAP_SERVERS)));

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    //
    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenRouteForLabelIsInvalid() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation("random", true);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void fetchRouteHostDetailsListReturnsEmptyWhenHostHasNoSubdomain() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(BOOTSTRAP.toString(), true);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost("random"))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).isEmpty();
    }

    @Test
    void fetchRouteHostDetailsListReturnsDetailsForValidBootstrapRoute() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(BOOTSTRAP.toString(), true);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).hasSize(1).satisfiesExactly(details -> {
            assertThat(details.routeNamespace()).isEqualTo(NAMESPACE);
            assertThat(details.clusterName()).isEqualTo(CLUSTER_NAME);
            assertThat(details.ingressName()).isEqualTo(INGRESS_NAME);
            assertThat(details.routeFor()).isEqualTo(BOOTSTRAP);
            assertThat(details.hostWithoutSubdomain()).isEqualTo(HOST_WITHOUT_SUBDOMAIN);
        });
    }

    @Test
    void fetchRouteHostDetailsListReturnsDetailsForValidNodeRoute() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(NODE.toString(), true);

        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(NODE_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).hasSize(1).satisfiesExactly(details -> {
            assertThat(details.routeFor()).isEqualTo(NODE);
            assertThat(details.hostWithoutSubdomain()).isEqualTo(HOST_WITHOUT_SUBDOMAIN);
        });
    }

    @Test
    void fetchRouteHostDetailsListStripsOnlyFirstHostSegment() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(NODE.toString(), true);

        String hostWithFourSegments = "sub.apps.cluster.example.com";
        String hostWithThreeSegments = "apps.cluster.example.com";

        // e.g. "a.b.c.d" → "b.c.d"
        //@formatter:off
        Route route = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                    .withIngress(ingressWithHost(hostWithFourSegments))
                .endStatus()
                .build();
        //@formatter:on

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(route));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).hasSize(1);
        assertThat(result.get(0).hostWithoutSubdomain()).isEqualTo(hostWithThreeSegments);
    }

    @Test
    void fetchRouteHostDetailsListFiltersOutInvalidRoutes() {
        // given
        ObjectMetaBuilder metaBuilder = metaWithRouteForValueAndBootstrapAnnotation(NODE.toString(), false);

        // no annotation
        Route invalidRoute = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();

        Annotations.annotateWithBootstrapServers(metaBuilder,
                Set.of(new Annotations.ClusterIngressBootstrapServers(CLUSTER_NAME, INGRESS_NAME, UNRESOLVED_HOST_BOOTSTRAP_SERVERS)));

        Route validRoute = new RouteBuilder()
                .withMetadata(metaBuilder.build())
                .withNewStatus()
                .withIngress(ingressWithHost(BOOTSTRAP_HOST_WITH_SUBDOMAIN))
                .endStatus()
                .build();

        // when
        when(context.getSecondaryResources(Route.class)).thenReturn(Set.of(invalidRoute, validRoute));

        List<RouteHostDetails> result = RouteHostDetails.fetchRouteHostDetailsList(context);

        // then
        assertThat(result).hasSize(1);
        assertThat(result.get(0).clusterName()).isEqualTo(CLUSTER_NAME);
    }

    private static ObjectMetaBuilder metaWithRouteForValueAndBootstrapAnnotation(String routeForValue, boolean includeBootstrapAnnoation) {
        var metaBuilder = new ObjectMetaBuilder()
                .withNamespace(NAMESPACE)
                .addToLabels(commonLabels())
                .addToLabels(Map.of(RouteHostDetails.RouteFor.LABEL_KEY, routeForValue));

        if (includeBootstrapAnnoation) {
            Annotations.annotateWithBootstrapServers(metaBuilder,
                    Set.of(new Annotations.ClusterIngressBootstrapServers(CLUSTER_NAME, INGRESS_NAME, UNRESOLVED_HOST_BOOTSTRAP_SERVERS)));
        }

        return metaBuilder;
    }

    private static RouteIngress ingressWithHost(String host) {
        return new RouteIngressBuilder().withHost(host).build();
    }

    private static @NonNull Map<String, String> commonLabels() {
        Map<String, String> orderedLabels = new LinkedHashMap<>();
        orderedLabels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        orderedLabels.put("app.kubernetes.io/name", "kroxylicious");
        orderedLabels.put("app.kubernetes.io/component", "proxy");
        orderedLabels.put("app.kubernetes.io/instance", PROXY_NAME);
        return orderedLabels;
    }
}