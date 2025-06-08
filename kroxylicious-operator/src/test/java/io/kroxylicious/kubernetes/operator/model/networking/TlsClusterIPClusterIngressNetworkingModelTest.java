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

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TlsClusterIPClusterIngressNetworkingModelTest {

    private static final String INGRESS_NAME = "my-ingress";
    public static final String CLUSTER_NAME = "my-cluster";
    public static final String PROXY_NAME = "my-proxy";
    public static final String NAMESPACE = "my-namespace";

    // @formatter:off
    private static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewClusterIP()
                    .withProtocol(ClusterIP.Protocol.TLS)
                .endClusterIP()
            .endSpec()
            .build();
    // @formatter:on

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
    public static final String PROXY_UID = "my-proxy-uid";
    public static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).withUid(PROXY_UID).withNamespace(NAMESPACE).endMetadata()
            .build();
    public static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);

    @Test
    void createInstancesWithSharedSniPort() {
        // given
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange("a", 1L, 3L));
        // when
        TlsClusterIPClusterIngressNetworkingModel model = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, TLS, 1);
        // then
        assertThat(model).isNotNull();
    }

    @Test
    void gatewayConfig() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));

        int sharedSniPort = 5;
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, TLS, sharedSniPort);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isNotNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(sni -> {
            assertThat(sni.bootstrapAddress()).isEqualTo(new HostPort("my-cluster-my-ingress-bootstrap." + NAMESPACE + ".svc.cluster.local", sharedSniPort).toString());
            assertThat(sni.advertisedBrokerAddressPattern()).isEqualTo("my-cluster-my-ingress-$(nodeId)." + NAMESPACE + ".svc.cluster.local:" + 9292);
        });
    }

    @Test
    void serviceMetadata() {
        // given
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), TLS, 5);
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
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), TLS, 5);

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().allSatisfy(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
                LinkedHashMap<String, String> orderedSelectorLabels = new LinkedHashMap<>();
                orderedSelectorLabels.put("app", "kroxylicious");
                orderedSelectorLabels.putAll(commonLabels(PROXY_NAME));
                assertThat(serviceSpec.getSelector()).containsExactlyEntriesOf(orderedSelectorLabels);
            });
        });
    }

    @Test
    void servicesSingleRangeOfNodeIds() {
        // given
        int sharedSniPort = 5;
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 2)), TLS, sharedSniPort);

        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        var listAssert = assertThat(serviceBuilders).isNotNull().hasSize(3);

        listAssert.element(0).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap", 9292, sharedSniPort);
        });

        listAssert.element(1).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-1", 9292, sharedSniPort);
        });

        listAssert.element(2).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-2", 9292, sharedSniPort);
        });
    }

    private static void assertServiceNameAndPortMapping(ServiceBuilder serviceBuild, String expectedName, int port, int targetPort) {
        Service build = serviceBuild.build();
        assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
            assertThat(metadata.getName()).isEqualTo(expectedName);
        });
        assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
            assertThat(serviceSpec.getPorts())
                    .isNotNull()
                    .isNotEmpty()
                    .containsExactly(new ServicePortBuilder()
                            .withTargetPort(new IntOrString(targetPort))
                            .withPort(port)
                            .withProtocol("TCP")
                            .build());
        });
    }

    @Test
    void servicesMultipleRangesOfNodeIds() {
        // given
        int sharedSniPort = 5;
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 1), createNodeIdRange("b", 3, 3)), TLS, sharedSniPort);

        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        var listAssert = assertThat(serviceBuilders).isNotNull().hasSize(3);

        listAssert.element(0).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap", 9292, sharedSniPort);
        });

        listAssert.element(1).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-1", 9292, sharedSniPort);
        });

        listAssert.element(2).satisfies(serviceBuild -> {
            assertServiceNameAndPortMapping(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-3", 9292, sharedSniPort);
        });
    }

    @Test
    void nodeIdRangesMustBeNonEmpty() {
        List<NodeIdRanges> nodeIdRanges = List.of();
        assertThatThrownBy(() -> new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, nodeIdRanges, TLS, 5))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void requiresSharedSniContainerPort() {
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), TLS, 5);

        assertThat(instance.requiresSharedSniContainerPort()).isTrue();
    }

    @Test
    void identifyingProxyContainerPortsEmpty() {
        ClusterIngressNetworkingModel instance = new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), TLS, 5);

        assertThat(instance).isNotNull();
        assertThat(instance.identifyingProxyContainerPorts())
                .isEmpty();
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        List<NodeIdRanges> nodeIdRanges = List.of(NODE_ID_RANGE);
        ThrowingCallable nullIngress = () -> new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, null, nodeIdRanges, TLS, 5);
        ThrowingCallable nullVirtualCluster = () -> new TlsClusterIPClusterIngressNetworkingModel(PROXY, null, INGRESS, nodeIdRanges, TLS, 5);
        ThrowingCallable nullProxy = () -> new TlsClusterIPClusterIngressNetworkingModel(null, VIRTUAL_KAFKA_CLUSTER, INGRESS, nodeIdRanges, TLS, 5);
        ThrowingCallable nullRanges = () -> new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, null, TLS, 5);
        ThrowingCallable nullTls = () -> new TlsClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, null, null, 5);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("virtualCluster", nullVirtualCluster),
                argumentSet("proxy", nullProxy),
                argumentSet("ranges", nullRanges),
                argumentSet("tls", nullTls));
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsMustBeNonNull(ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(NullPointerException.class);
    }

    private static @NonNull Map<String, String> commonLabels(String proxyName) {
        Map<String, String> orderedLabels = new LinkedHashMap<>();
        orderedLabels.put("app.kubernetes.io/part-of", "kafka");
        orderedLabels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        orderedLabels.put("app.kubernetes.io/name", "kroxylicious-proxy");
        orderedLabels.put("app.kubernetes.io/instance", proxyName);
        orderedLabels.put("app.kubernetes.io/component", "proxy");
        return orderedLabels;
    }

    private static NodeIdRanges createNodeIdRange(@Nullable String name, long start, long endInclusive) {
        return new NodeIdRangesBuilder().withName(name).withStart(start).withEnd(endInclusive).build();
    }

}
