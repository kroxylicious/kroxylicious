/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;
import io.fabric8.openshift.api.model.RouteSpec;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoutes;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoutesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class OpenShiftRoutesClusterIngressNetworkingModelTest {
    private static final String INGRESS_NAME = "my-ingress";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String NAMESPACE = "my-namespace";

    private static final String INGRESS_DOMAIN = "ingress.openshiftapps.com";

    // @formatter:off
    private static final OpenShiftRoutes OPEN_SHIFT_ROUTES = new OpenShiftRoutesBuilder()
            .withIngressDomain(INGRESS_DOMAIN)
            .build();

    private static final String PROXY_NAME = "my-proxy";
    private static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName(PROXY_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

    private static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withOpenShiftRoutes(OPEN_SHIFT_ROUTES)
            .endSpec()
            .build();

    private static final Tls TLS = new TlsBuilder()
            .withNewCertificateRef()
                .withName("server-cert")
            .endCertificateRef()
            .build();

    private static final VirtualKafkaCluster VIRTUAL_KAFKA_CLUSTER = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .addNewIngress()
                    .withNewIngressRef()
                        .withName(INGRESS_NAME)
                    .endIngressRef()
                    .withTls(TLS)
                .endIngress()
            .endSpec()
            .build();
    // @formatter:on
    public static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);

    @Test
    void gatewayConfig() {
        // given
        int sharedSniPort = 1;
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, sharedSniPort);

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(model).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isNotNull();
        assertThat(gateway.portIdentifiesNode()).isNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(sniStrategy -> {
            assertThat(sniStrategy.bootstrapAddress()).isEqualTo(
                    new HostPort(expectedRouteHostname("bootstrap", CLUSTER_NAME, INGRESS_NAME, INGRESS_DOMAIN), sharedSniPort).toString());
            assertThat(sniStrategy.advertisedBrokerAddressPattern()).isEqualTo(
                    new HostPort(expectedRouteHostname("broker-$(nodeId)", CLUSTER_NAME, INGRESS_NAME, INGRESS_DOMAIN), 443).toString());
        });
    }

    @Test
    void doesNotSupplyAnyServices() {
        // given
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);
        assertThat(model).isNotNull();

        // when
        Stream<ServiceBuilder> services = model.services();

        // then
        assertThat(services).isEmpty();
    }

    @Test
    void requestsRoutes() {
        // given
        int sharedSniPort = 9093;
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, sharedSniPort);
        assertThat(model).isNotNull();

        // when
        List<RouteBuilder> routes = model.routes().toList();

        // then
        List<Route> builtRoutes = routes.stream().map(RouteBuilder::build).toList();
        var listAssert = assertThat(builtRoutes);
        listAssert.hasSize(4);
        listAssert.element(0).satisfies(route -> {
            assertRoute(route, CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap", "bootstrap", sharedSniPort);
        });
        listAssert.element(1).satisfies(route -> {
            assertRoute(route, CLUSTER_NAME + "-" + INGRESS_NAME + "-1", "broker-1", sharedSniPort);
        });
        listAssert.element(2).satisfies(route -> {
            assertRoute(route, CLUSTER_NAME + "-" + INGRESS_NAME + "-2", "broker-2", sharedSniPort);
        });
        listAssert.element(3).satisfies(route -> {
            assertRoute(route, CLUSTER_NAME + "-" + INGRESS_NAME + "-3", "broker-3", sharedSniPort);
        });
    }

    private static void assertRoute(Route route, String routeName, String routePrefix, int sharedSniPort) {
        Map<String, String> orderedStandardLabels = commonLabels(PROXY_NAME);
        OperatorAssertions.assertThat(route.getMetadata())
                .hasName(routeName)
                .hasNamespace(NAMESPACE)
                .hasLabels(orderedStandardLabels)
                .hasOwnerRefs(ResourcesUtil.newOwnerReferenceTo(PROXY), ResourcesUtil.newOwnerReferenceTo(INGRESS),
                        ResourcesUtil.newOwnerReferenceTo(VIRTUAL_KAFKA_CLUSTER));
        RouteSpec spec = route.getSpec();
        assertThat(spec.getHost()).isEqualTo(expectedRouteHostname(routePrefix, CLUSTER_NAME, INGRESS_NAME, INGRESS_DOMAIN));
        assertThat(spec.getTo()).satisfies(routeTarget -> {
            assertThat(routeTarget.getKind()).isEqualTo("Service");
            assertThat(routeTarget.getName()).isEqualTo(PROXY_NAME + "-sni");
        });
        assertThat(spec.getPort().getTargetPort()).isEqualTo(new IntOrString(sharedSniPort));
        assertThat(spec.getTls()).isNotNull().satisfies(tlsConfig -> {
            assertThat(tlsConfig.getTermination()).isEqualTo("passthrough");
            assertThat(tlsConfig.getInsecureEdgeTerminationPolicy()).isEqualTo("None");
        });
    }

    @Test
    void requestsLoadBalancerServicePorts() {
        // given
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);
        assertThat(model).isNotNull();

        // when
        var services = model.sharedLoadBalancerServiceRequirements();

        // then
        assertThat(services).isPresent();
        assertThat(services.get().requiredClientFacingPorts()).containsExactly(9083);
    }

    @Test
    void requestsSharedSniPort() {
        // given
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);

        // when
        boolean requiresSharedSniPort = model.requiresSharedSniContainerPort();

        // then
        assertThat(requiresSharedSniPort).isTrue();
    }

    @Test
    void sharedLoadBalancerServiceBootstrapServers() {
        // given
        OpenShiftRoutesClusterIngressNetworkingModel model = new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);

        // when
        Optional<SharedLoadBalancerServiceRequirements> requirements = model.sharedLoadBalancerServiceRequirements();

        // then
        assertThat(requirements).isPresent();
        assertThat(requirements.get().bootstrapServersToAnnotate()).satisfies(bootstrapServers -> {
            assertThat(bootstrapServers.clusterName()).isEqualTo(CLUSTER_NAME);
            assertThat(bootstrapServers.ingressName()).isEqualTo(INGRESS_NAME);
            assertThat(bootstrapServers.bootstrapServers()).isEqualTo(expectedRouteHostname("bootstrap", CLUSTER_NAME, INGRESS_NAME, INGRESS_DOMAIN) + ":443");
        });
    }

    private static String expectedRouteHostname(String prefix, String clusterName, String ingressName, String ingressDomain) {
        return prefix + "." + clusterName + "-" + ingressName + "." + ingressDomain;
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        ThrowableAssert.ThrowingCallable nullIngress = () -> new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, null, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);
        ThrowableAssert.ThrowingCallable nullCluster = () -> new OpenShiftRoutesClusterIngressNetworkingModel(null, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);
        ThrowableAssert.ThrowingCallable nullRoutes = () -> new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, null,
                List.of(NODE_ID_RANGE), TLS, 9093);
        ThrowableAssert.ThrowingCallable nullTls = () -> new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), null, 9093);
        ThrowableAssert.ThrowingCallable nullNodeIdRanges = () -> new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, PROXY,
                OPEN_SHIFT_ROUTES, null, TLS, 9093);
        ThrowableAssert.ThrowingCallable nullProxy = () -> new OpenShiftRoutesClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, null, OPEN_SHIFT_ROUTES,
                List.of(NODE_ID_RANGE), TLS, 9093);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("cluster", nullCluster),
                argumentSet("openShiftRoutes", nullRoutes),
                argumentSet("nnodeIdRanges", nullNodeIdRanges),
                argumentSet("proxy", nullProxy),
                argumentSet("tls", nullTls));
    }

    private static @NonNull Map<String, String> commonLabels(String proxyName) {
        Map<String, String> orderedLabels = new java.util.LinkedHashMap<>();
        orderedLabels.put("app.kubernetes.io/part-of", "kafka");
        orderedLabels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        orderedLabels.put("app.kubernetes.io/name", "kroxylicious-proxy");
        orderedLabels.put("app.kubernetes.io/instance", proxyName);
        orderedLabels.put("app.kubernetes.io/component", "proxy");
        return orderedLabels;
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsMustBeNonNull(ThrowableAssert.ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(NullPointerException.class);
    }

    private static NodeIdRanges createNodeIdRange(@Nullable String name, long start, long endInclusive) {
        return new NodeIdRangesBuilder().withName(name).withStart(start).withEnd(endInclusive).build();
    }
}
