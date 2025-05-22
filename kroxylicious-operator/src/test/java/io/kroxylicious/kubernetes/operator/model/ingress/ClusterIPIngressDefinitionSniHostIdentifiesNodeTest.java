/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.assertj.core.api.ListAssert;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
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
import io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ClusterIPIngressDefinitionSniHostIdentifiesNodeTest {

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
                    .withNodeIdentifiedBy(ClusterIP.NodeIdentifiedBy.SNI)
                .endClusterIP()
            .endSpec()
            .build();
    // @formatter:on

    public static final String CERTIFICATE_NAME = "certificate";
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
            .withNewTls()
            .withNewCertificateRef()
            .withName(CERTIFICATE_NAME)
            .endCertificateRef()
            .endTls()
            .endIngress()
            .endSpec()
            .build();
    // @formatter:on
    public static final String PROXY_UID = "my-proxy-uid";
    public static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).withUid(PROXY_UID).withNamespace(NAMESPACE).endMetadata()
            .build();
    public static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);
    public static final int SHARED_TLS_PORT = 9191;

    @Test
    void numIdentifyingPortsIsZero() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(NODE_ID_RANGE), null);
        // 3 ports for the nodeId range + 1 for bootstrap
        assertThat(definition.numIdentifyingPortsRequired()).isEqualTo(0);
    }

    @Test
    void requiresASharedTlsPort() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(NODE_ID_RANGE), null);
        // 3 ports for the nodeId range + 1 for bootstrap
        assertThat(definition.requiresSharedTLSPort()).isTrue();
    }

    @Test
    void createInstancesWithSharedPort() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)), null);
        // when
        // then
        // expect to be given a shared TLS port
        assertThat(definition.createIngressModel(null, null, SHARED_TLS_PORT)).isNotNull();
    }

    @Test
    void createdInstanceRequiresASharedTlsPort() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)), null);
        // when
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        // then
        // expect to be given a shared TLS port
        assertThat(instance.requiresSharedTLSPort()).isTrue();
    }

    @Test
    void createInstancesWithNoSharedPort() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)), null);
        // when
        // then
        // expect a non null TLS port
        assertThatThrownBy(() -> definition.createIngressModel(null, null, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void gatewayConfig() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        Tls tls = new TlsBuilder().withNewCertificateRef().withName("cert").endCertificateRef().build();
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd)), tls);
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo("default");
        assertThat(gateway.tls()).isPresent();
        assertThat(gateway.portIdentifiesNode()).isNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(strategy -> {
            assertThat(strategy.advertisedBrokerAddressPattern()).isEqualTo(CLUSTER_NAME + "-" + INGRESS_NAME + "-$(nodeId).my-namespace.svc.cluster.local");
            assertThat(strategy.bootstrapAddress()).isEqualTo(new HostPort(CLUSTER_NAME + "-" + INGRESS_NAME + ".my-namespace.svc.cluster.local",
                    ProxyDeploymentDependentResource.PROXY_SHARED_TLS_PORT).toString());
        });
    }

    @Test
    void serviceMetadata() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 1)), null);
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        ListAssert<ServiceBuilder> serviceBuilderListAssert = assertThat(serviceBuilders).isNotNull().isNotEmpty().hasSize(2);
        serviceBuilderListAssert.element(0).satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertServiceMetadata(metadata, CLUSTER_NAME + "-" + INGRESS_NAME + "-1");
            });
        });
        serviceBuilderListAssert.element(1).satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertServiceMetadata(metadata, CLUSTER_NAME + "-" + INGRESS_NAME);
            });
        });
    }

    private static void assertServiceMetadata(ObjectMeta metadata, String serviceName) {
        Map<String, String> orderedServiceLabels = commonLabels(PROXY_NAME);
        assertThat(metadata.getNamespace()).isEqualTo(NAMESPACE);
        assertThat(metadata.getName()).isEqualTo(serviceName);
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
    }

    @Test
    void serviceCommonSpec() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 3)), null);
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();

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
    void servicesSingleRange() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 2)), null);
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        ListAssert<ServiceBuilder> serviceBuilderListAssert = assertThat(serviceBuilders).isNotNull().isNotEmpty().hasSize(3);
        serviceBuilderListAssert.element(0).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-1");
        });
        serviceBuilderListAssert.element(1).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-2");
        });
        serviceBuilderListAssert.element(2).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME);
        });
    }

    @Test
    void servicesMultipleRanges() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 1), createNodeIdRange("b", 3, 3)), null);
        // 2 ports for the nodeId ranges + 1 for bootstrap
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        ListAssert<ServiceBuilder> serviceBuilderListAssert = assertThat(serviceBuilders).isNotNull().isNotEmpty().hasSize(3);
        serviceBuilderListAssert.element(0).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-1");
        });
        serviceBuilderListAssert.element(1).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME + "-3");
        });
        serviceBuilderListAssert.element(2).satisfies(serviceBuild -> {
            assertServicePorts(serviceBuild, CLUSTER_NAME + "-" + INGRESS_NAME);
        });
    }

    private void assertServicePorts(ServiceBuilder serviceBuild, String serviceName) {
        Service build = serviceBuild.build();
        assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
            assertThat(metadata.getName()).isEqualTo(serviceName);
        });
        assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
            assertThat(serviceSpec.getPorts()).containsExactly(createTcpServicePort("kafka-tls-sni", SHARED_TLS_PORT));
        });
    }

    @Test
    void rangesMustBeNonEmpty() {
        List<NodeIdRanges> nodeIdRanges = List.of();
        assertThatThrownBy(() -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY, nodeIdRanges, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void proxyContainerPortsShouldAlwaysBeEmpty() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)), null);
        IngressModel instance = definition.createIngressModel(null, null, SHARED_TLS_PORT);
        assertThat(instance).isNotNull();
        assertThat(instance.proxyContainerPorts()).isEmpty();
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        List<NodeIdRanges> nodeIdRanges = List.of(NODE_ID_RANGE);
        ThrowingCallable nullIngress = () -> new ClusterIPIngressDefinition(null, VIRTUAL_KAFKA_CLUSTER, PROXY, nodeIdRanges, null);
        ThrowingCallable nullVirtualCluster = () -> new ClusterIPIngressDefinition(INGRESS, null, PROXY, nodeIdRanges, null);
        ThrowingCallable nullProxy = () -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, null, nodeIdRanges, null);
        ThrowingCallable nullRanges = () -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY, null, null);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("virtualCluster", nullVirtualCluster),
                argumentSet("proxy", nullProxy),
                argumentSet("ranges", nullRanges));
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsMustBeNonNull(ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(NullPointerException.class);
    }

    private ServicePort createTcpServicePort(String portName, int port) {
        return new ServicePortBuilder()
                .withName(portName)
                .withTargetPort(new IntOrString(port))
                .withPort(port)
                .withProtocol("TCP")
                .build();
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
