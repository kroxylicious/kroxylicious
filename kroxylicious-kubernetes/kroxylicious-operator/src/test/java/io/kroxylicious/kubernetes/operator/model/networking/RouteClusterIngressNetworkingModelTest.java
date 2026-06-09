/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.openshift.api.model.Route;
import io.fabric8.openshift.api.model.RouteBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.OpenShiftRoute;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.model.RouteHostDetails;
import io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy.KafkaProxyReconciler;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.as;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class RouteClusterIngressNetworkingModelTest {
    private static final String INGRESS_NAME = "my-ingress";
    private static final String PROXY_NAME = "my-proxy";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String NAMESPACE = "my-namespace";

    // @formatter:off
    private static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewOpenShiftRoute()
                .endOpenShiftRoute()
                .withNewProxyRef()
                    .withName(PROXY_NAME)
                .endProxyRef()
            .endSpec()
            .build();
    // @formatter:on

    private static final OpenShiftRoute ROUTE = INGRESS.getSpec().getOpenShiftRoute();
    private static final int HTTPS_PORT = 443;

    private static final Tls TLS = new TlsBuilder().withNewCertificateRef().withName("server-cert").endCertificateRef().build();

    // @formatter:off
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

    // @formatter:off
    public static final String PROXY_UID = "my-proxy-uid";
    public static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName(PROXY_NAME)
                .withUid(PROXY_UID)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();
    // @formatter:on

    public static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);

    @Test
    void createInstancesWithSharedSniPort() {
        // given
        // when
        var model = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 1, List.of());
        // then
        assertThat(model).isNotNull();
    }

    @Test
    void gatewayConfigWithUnresolvedHost() {
        // given
        int sharedSniPort = 5;

        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));

        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, nodeIdRange, TLS, sharedSniPort,
                List.of());
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isNotNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(sni -> {
            assertThat(sni.getBootstrapAddress())
                    .isEqualTo(new HostPort("$(virtualClusterName)-bootstrap.$(unresolvedRouteHost)", sharedSniPort).toString());
            assertThat(sni.getAdvertisedBrokerAddressPattern()).isEqualTo("$(virtualClusterName)-$(nodeId).$(unresolvedRouteHost):" + HTTPS_PORT);
        });
    }

    @Test
    void gatewayConfigWithResolvedHost() {
        // given
        int sharedSniPort = 5;

        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));

        String bootstrapHostWithoutSubdomain = "apps.bootstrap.example.com";
        String nodeHostWithoutSubdomain = "apps.node.example.com";
        var bootstrapRoute = new RouteHostDetails(NAMESPACE, CLUSTER_NAME, INGRESS_NAME, RouteHostDetails.RouteFor.BOOTSTRAP, bootstrapHostWithoutSubdomain);
        var nodeRoute = new RouteHostDetails(NAMESPACE, CLUSTER_NAME, INGRESS_NAME, RouteHostDetails.RouteFor.NODE, nodeHostWithoutSubdomain);
        List<RouteHostDetails> routeHostDetails = List.of(bootstrapRoute, nodeRoute);

        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, nodeIdRange, TLS, sharedSniPort,
                routeHostDetails);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isNotNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(sni -> {
            assertThat(sni.getBootstrapAddress())
                    .isEqualTo(new HostPort("$(virtualClusterName)-bootstrap." + bootstrapHostWithoutSubdomain, sharedSniPort).toString());
            assertThat(sni.getAdvertisedBrokerAddressPattern()).isEqualTo("$(virtualClusterName)-$(nodeId)." + nodeHostWithoutSubdomain + ":" + HTTPS_PORT);
        });
    }

    @Test
    void serviceMetadata() {
        // given
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().allSatisfy(serviceBuild -> {
            Service build = serviceBuild.build();
            Map<String, String> orderedServiceLabels = commonLabels(PROXY_NAME);
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertThat(metadata.getNamespace()).isEqualTo(NAMESPACE);
                assertThat(metadata.getLabels()).containsExactlyEntriesOf(orderedServiceLabels);
                assertThat(metadata.getAnnotations()).satisfies(am -> {
                    assertThat(am)
                            .containsOnlyKeys(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY)
                            .extractingByKey(Annotations.BOOTSTRAP_SERVERS_ANNOTATION_KEY, as(InstanceOfAssertFactories.STRING))
                            .contains("my-cluster-bootstrap.$(unresolvedRouteHost):443");
                });
                assertThat(metadata.getOwnerReferences())
                        .satisfiesExactlyInAnyOrder(
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("KafkaProxy");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(PROXY_NAME);
                                    assertThat(ownerRef.getUid()).isEqualTo(PROXY_UID);
                                },
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("VirtualKafkaCluster");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(CLUSTER_NAME);
                                },
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("KafkaProxyIngress");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(INGRESS_NAME);
                                });
            });
        });
    }

    @Test
    void serviceCommonSpec() {
        // given
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().allSatisfy(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
                LinkedHashMap<String, String> orderedSelectorLabels = new LinkedHashMap<>(commonLabels(PROXY_NAME));
                assertThat(serviceSpec.getSelector()).containsExactlyEntriesOf(orderedSelectorLabels);
            });
        });
    }

    @Test
    void servicesSinglePort() {
        // given
        int sharedSniPort = 5;
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS,
                sharedSniPort, List.of());

        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        var listAssert = assertThat(serviceBuilders).isNotNull().hasSize(1);

        listAssert.element(0)
                .satisfies(serviceBuild -> assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-service",
                        CLUSTER_NAME + "-" + sharedSniPort, sharedSniPort,
                        sharedSniPort));
    }

    private static void assertServiceNameAndPortMapping(ServiceBuilder serviceBuild, String expectedServiceName, String expectedPortName, int port, int targetPort) {
        Service build = serviceBuild.build();
        assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> assertThat(metadata.getName()).isEqualTo(expectedServiceName));
        assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> assertThat(serviceSpec.getPorts())
                .isNotNull()
                .isNotEmpty()
                .containsExactly(new ServicePortBuilder()
                        .withName(expectedPortName)
                        .withTargetPort(new IntOrString(targetPort))
                        .withPort(port)
                        .withProtocol("TCP")
                        .build()));
    }

    @Test
    void routeMetadata() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));

        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, nodeIdRange, TLS, 5, List.of());
        assertThat(instance).isNotNull();

        // when
        List<RouteBuilder> routeBuilders = instance.routes().toList();

        var listAssert = assertThat(routeBuilders).isNotNull().hasSize(rangeEnd + 1);

        // then
        listAssert.allSatisfy(routeBuild -> {
            Route build = routeBuild.build();
            LinkedHashMap<String, String> orderedRouteLabels = commonLabels(PROXY_NAME);
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertThat(metadata.getNamespace()).isEqualTo(NAMESPACE);
                assertThat(metadata.getLabels()).containsAllEntriesOf(orderedRouteLabels);
                assertThat(metadata.getOwnerReferences())
                        .satisfiesExactlyInAnyOrder(
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("KafkaProxy");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(PROXY_NAME);
                                    assertThat(ownerRef.getUid()).isEqualTo(PROXY_UID);
                                },
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("VirtualKafkaCluster");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(CLUSTER_NAME);
                                },
                                ownerRef -> {
                                    assertThat(ownerRef.getKind()).isEqualTo("KafkaProxyIngress");
                                    assertThat(ownerRef.getApiVersion()).isEqualTo("kroxylicious.io/v1alpha1");
                                    assertThat(ownerRef.getName()).isEqualTo(INGRESS_NAME);
                                });
            });
        });

        listAssert.satisfiesExactly(
                routeBuild -> routeBuildContainsMatchingNameAndRouteForValue(routeBuild, CLUSTER_NAME + "-bootstrap", RouteHostDetails.RouteFor.BOOTSTRAP),
                routeBuild -> routeBuildContainsMatchingNameAndRouteForValue(routeBuild, CLUSTER_NAME + "-1", RouteHostDetails.RouteFor.NODE),
                routeBuild -> routeBuildContainsMatchingNameAndRouteForValue(routeBuild, CLUSTER_NAME + "-2", RouteHostDetails.RouteFor.NODE),
                routeBuild -> routeBuildContainsMatchingNameAndRouteForValue(routeBuild, CLUSTER_NAME + "-3", RouteHostDetails.RouteFor.NODE));
    }

    private static void routeBuildContainsMatchingNameAndRouteForValue(RouteBuilder routeBuilder, String routeName, RouteHostDetails.RouteFor routeFor) {
        Route build = routeBuilder.build();
        LinkedHashMap<String, String> orderedRouteLabels = commonLabels(PROXY_NAME);
        orderedRouteLabels.put(RouteHostDetails.RouteFor.LABEL_KEY, String.valueOf(routeFor));
        assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
            assertThat(metadata.getName()).isEqualTo(routeName);
            assertThat(metadata.getLabels()).containsExactlyEntriesOf(orderedRouteLabels);
        });
    }

    @Test
    void routeSpec() {
        // given
        int sharedSniPort = 5;
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, nodeIdRange, TLS, sharedSniPort,
                List.of());

        // when
        List<RouteBuilder> routeBuilders = instance.routes().toList();

        var listAssert = assertThat(routeBuilders).isNotNull().hasSize(rangeEnd + 1);

        // then
        listAssert.allSatisfy(routeBuild -> {
            Route build = routeBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(routeSpec -> {
                assertThat(routeSpec.getPort()).isNotNull()
                        .satisfies(routePort -> assertThat(routePort.getTargetPort()).isNotNull().isEqualTo(new IntOrString(sharedSniPort)));
                assertThat(routeSpec.getTo()).isNotNull().satisfies(routeTargetReference -> {
                    assertThat(routeTargetReference.getKind()).isNotNull().isEqualTo("Service");
                    assertThat(routeTargetReference.getName()).isNotNull().isEqualTo(CLUSTER_NAME + "-" + INGRESS_NAME + "-service");
                });
                assertThat(routeSpec.getTls()).isNotNull().satisfies(tlsConfig -> assertThat(tlsConfig.getTermination()).isNotNull().isEqualTo("passthrough"));
            });
        });

        listAssert.satisfiesExactly(
                routeBuild -> routeBuildSpecContainsSubdomain(routeBuild, CLUSTER_NAME + "-bootstrap"),
                routeBuild -> routeBuildSpecContainsSubdomain(routeBuild, CLUSTER_NAME + "-1"),
                routeBuild -> routeBuildSpecContainsSubdomain(routeBuild, CLUSTER_NAME + "-2"),
                routeBuild -> routeBuildSpecContainsSubdomain(routeBuild, CLUSTER_NAME + "-3"));
    }

    private static void routeBuildSpecContainsSubdomain(RouteBuilder routeBuilder, String subdomain) {
        Route build = routeBuilder.build();
        assertThat(build.getSpec()).isNotNull().satisfies(routeSpec -> assertThat(routeSpec.getSubdomain()).isNotNull().isEqualTo(subdomain));
    }

    @SuppressWarnings("java:S5778") // Empty RouteHostDetails list won't cause an exception
    @Test
    void nodeIdRangesMustBeNonEmpty() {
        List<NodeIdRanges> nodeIdRanges = List.of();
        assertThatThrownBy(() -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, nodeIdRanges, TLS, 5, List.of()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresSharedSniContainerPort() {
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());

        assertThat(instance.requiresSharedSniContainerPort()).isTrue();
    }

    @Test
    void identifyingProxyContainerPortsEmpty() {
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());

        assertThat(instance).isNotNull();
        assertThat(instance.identifyingProxyContainerPorts())
                .isEmpty();
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        ThrowableAssert.ThrowingCallable nullIngress = () -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, null, ROUTE, List.of(NODE_ID_RANGE),
                TLS, 5, List.of());
        ThrowableAssert.ThrowingCallable nullVirtualCluster = () -> new RouteClusterIngressNetworkingModel(PROXY, null, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());
        ThrowableAssert.ThrowingCallable nullProxy = () -> new RouteClusterIngressNetworkingModel(null, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE),
                TLS, 5, List.of());
        ThrowableAssert.ThrowingCallable nullRanges = () -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, null, TLS, 5, List.of());
        ThrowableAssert.ThrowingCallable nullTls = () -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE),
                null, 5, List.of());
        ThrowableAssert.ThrowingCallable nullRoute = () -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, null, List.of(NODE_ID_RANGE),
                TLS, 5, List.of());
        ThrowableAssert.ThrowingCallable nullRouteHostDetails = () -> new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE,
                List.of(NODE_ID_RANGE), TLS, 5, null);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("virtualCluster", nullVirtualCluster),
                argumentSet("proxy", nullProxy),
                argumentSet("ranges", nullRanges),
                argumentSet("tls", nullTls),
                argumentSet("route", nullRoute),
                argumentSet("routeHostDetails", nullRouteHostDetails));
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsMustBeNonNull(ThrowableAssert.ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(NullPointerException.class);
    }

    private static @NonNull LinkedHashMap<String, String> commonLabels(String proxyName) {
        LinkedHashMap<String, String> orderedLabels = new LinkedHashMap<>();
        orderedLabels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        orderedLabels.put("app.kubernetes.io/name", "kroxylicious");
        orderedLabels.put("app.kubernetes.io/component", "proxy");
        orderedLabels.put("app.kubernetes.io/instance", proxyName);
        return orderedLabels;
    }

    private static NodeIdRanges createNodeIdRange(@Nullable String name, long start, long endInclusive) {
        return new NodeIdRangesBuilder().withName(name).withStart(start).withEnd(endInclusive).build();
    }

    @Test
    void serviceIncludesInfrastructureAnnotations() {
        // given
        KafkaProxyIngress ingressWithAnnotations = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName(INGRESS_NAME)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withNewInfrastructure()
                .addToAnnotations("example.com/custom", "test-value")
                .addToAnnotations("haproxy.router.openshift.io/timeout", "60s")
                .endInfrastructure()
                .withNewOpenShiftRoute()
                .endOpenShiftRoute()
                .withNewProxyRef()
                .withName(PROXY_NAME)
                .endProxyRef()
                .endSpec()
                .build();

        // when
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, ingressWithAnnotations,
                ingressWithAnnotations.getSpec().getOpenShiftRoute(), List.of(NODE_ID_RANGE), TLS, 5, List.of());
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).singleElement().satisfies(serviceBuild -> {
            Service service = serviceBuild.build();
            assertThat(service.getMetadata().getAnnotations())
                    .containsEntry("example.com/custom", "test-value")
                    .containsEntry("haproxy.router.openshift.io/timeout", "60s")
                    .containsKey("kroxylicious.io/bootstrap-servers");
        });
    }

    @Test
    void serviceWithoutInfrastructureAnnotations() {
        // given - using INGRESS without infrastructure annotations
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then - only operator-managed annotation present
        assertThat(serviceBuilders).singleElement().satisfies(serviceBuild -> {
            Service service = serviceBuild.build();
            assertThat(service.getMetadata().getAnnotations())
                    .containsOnlyKeys("kroxylicious.io/bootstrap-servers");
        });
    }

    @Test
    void routeIncludesInfrastructureAnnotations() {
        // given
        KafkaProxyIngress ingressWithAnnotations = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName(INGRESS_NAME)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withNewInfrastructure()
                .addToAnnotations("example.com/custom", "test-value")
                .addToAnnotations("haproxy.router.openshift.io/timeout", "60s")
                .endInfrastructure()
                .withNewOpenShiftRoute()
                .endOpenShiftRoute()
                .withNewProxyRef()
                .withName(PROXY_NAME)
                .endProxyRef()
                .endSpec()
                .build();

        // when
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, ingressWithAnnotations,
                ingressWithAnnotations.getSpec().getOpenShiftRoute(), List.of(NODE_ID_RANGE), TLS, 5, List.of());
        List<RouteBuilder> routeBuilders = instance.routes().toList();

        // then
        assertThat(routeBuilders).isNotEmpty().allSatisfy(routeBuild -> {
            Route route = routeBuild.build();
            assertThat(route.getMetadata().getAnnotations())
                    .containsEntry("example.com/custom", "test-value")
                    .containsEntry("haproxy.router.openshift.io/timeout", "60s")
                    .containsKey("kroxylicious.io/bootstrap-servers");
        });
    }

    @Test
    void routeWithoutInfrastructureAnnotations() {
        // given - using INGRESS without infrastructure annotations
        ClusterIngressNetworkingModel instance = new RouteClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, ROUTE, List.of(NODE_ID_RANGE), TLS, 5,
                List.of());

        // when
        List<RouteBuilder> routeBuilders = instance.routes().toList();

        // then - only operator-managed annotation present
        assertThat(routeBuilders).isNotEmpty().allSatisfy(routeBuild -> {
            Route route = routeBuild.build();
            assertThat(route.getMetadata().getAnnotations())
                    .containsOnlyKeys("kroxylicious.io/bootstrap-servers");
        });
    }
}
