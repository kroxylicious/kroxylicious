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

import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
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
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.kubernetes.operator.model.ingress.ClusterIPIngressDefinition.ClusterIPIngressInstance;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class ClusterIPIngressDefinitionTest {

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
            .build();
    // @formatter:on

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
                .endIngress()
            .endSpec()
            .build();
    // @formatter:on
    public static final String PROXY_UID = "my-proxy-uid";
    public static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).withUid(PROXY_UID).withNamespace(NAMESPACE).endMetadata()
            .build();
    public static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);

    @Test
    void numIdentifyingPortsRequiredSingleRange() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(NODE_ID_RANGE));
        // 3 ports for the nodeId range + 1 for bootstrap
        assertThat(definition.numIdentifyingPortsRequired()).isEqualTo(4);
    }

    @Test
    void createInstancesWithExpectedNumber() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)));
        // when
        // then
        // expect to be allocated 4 ports total, 3 ports for the nodeId range + 1 for bootstrap
        assertThat(definition.createInstance(1, 4)).isNotNull();
    }

    @Test
    void createInstancesWithTooManyPorts() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)));
        // when
        // then
        // more ports than the 4 required by the node id range + 1 for bootstrap
        assertThatThrownBy(() -> definition.createInstance(1, 5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createInstancesWithNotEnoughPorts() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)));

        // when
        // then
        // more ports than the 4 required by the node id range + 1 for bootstrap
        assertThatThrownBy(() -> definition.createInstance(1, 3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void gatewayConfigSingleRange() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd)));
        // 3 ports for the nodeId range + 1 for bootstrap
        ClusterIPIngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo("default");
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.bootstrapAddress()).isEqualTo(new HostPort("localhost", 1));
            assertThat(portIdentifiesNode.nodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.advertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange(rangeName, rangeStart, rangeEnd);
            assertThat(portIdentifiesNode.nodeIdRanges()).containsExactly(expectedRange);
        });
    }

    @Test
    void serviceMetadata() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 3)));
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            Map<String, String> orderedServiceLabels = commonLabels(PROXY_NAME);
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertThat(metadata.getNamespace()).isEqualTo(NAMESPACE);
                assertThat(metadata.getName()).isEqualTo(CLUSTER_NAME + "-" + INGRESS_NAME);
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
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 3)));
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
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
    void servicesSingleRangePorts() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 3)));
        // 3 ports for the nodeId range + 1 for bootstrap
        IngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
                ServicePort port1 = createTcpServicePort(CLUSTER_NAME + "-" + 1, 1);
                ServicePort port2 = createTcpServicePort(CLUSTER_NAME + "-" + 2, 2);
                ServicePort port3 = createTcpServicePort(CLUSTER_NAME + "-" + 3, 3);
                ServicePort port4 = createTcpServicePort(CLUSTER_NAME + "-" + 4, 4);
                assertThat(serviceSpec.getPorts())
                        .isNotNull()
                        .isNotEmpty()
                        .containsExactlyInAnyOrder(port1, port2, port3, port4);
            });
        });
    }

    @Test
    void servicesMultipleRangesPorts() {
        // given
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1, 1), createNodeIdRange("b", 3, 3)));
        // 2 ports for the nodeId ranges + 1 for bootstrap
        IngressInstance instance = definition.createInstance(1, 3);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
                ServicePort port1 = createTcpServicePort(CLUSTER_NAME + "-" + 1, 1);
                ServicePort port2 = createTcpServicePort(CLUSTER_NAME + "-" + 2, 2);
                ServicePort port3 = createTcpServicePort(CLUSTER_NAME + "-" + 3, 3);
                assertThat(serviceSpec.getPorts())
                        .isNotNull()
                        .isNotEmpty()
                        .containsExactlyInAnyOrder(port1, port2, port3);
            });
        });
    }

    @Test
    void gatewayConfigNameDefault() {
        // given
        int rangeStart = 1;
        int rangeEnd = 3;

        int rangeStart2 = 5;
        int rangeEnd2 = 6;
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange(null, rangeStart, rangeEnd), createNodeIdRange(null, rangeStart2, rangeEnd2)));
        // 5 ports for the nodeId ranges + 1 for bootstrap
        ClusterIPIngressInstance instance = definition.createInstance(2, 7);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo("default");
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.bootstrapAddress()).isEqualTo(new HostPort("localhost", 2));
            assertThat(portIdentifiesNode.nodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.advertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange("range-0", rangeStart, rangeEnd);
            NamedRange expectedRange2 = new NamedRange("range-1", rangeStart2, rangeEnd2);
            assertThat(portIdentifiesNode.nodeIdRanges()).containsExactly(expectedRange, expectedRange2);
        });
    }

    @Test
    void gatewayConfigMultipleRange() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;

        String rangeName2 = "b";
        int rangeStart2 = 5;
        int rangeEnd2 = 6;
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd), createNodeIdRange(rangeName2, rangeStart2, rangeEnd2)));
        // 5 ports for the nodeId ranges + 1 for bootstrap
        ClusterIPIngressInstance instance = (ClusterIPIngressInstance) definition.createInstance(2, 7);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo("default");
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.bootstrapAddress()).isEqualTo(new HostPort("localhost", 2));
            assertThat(portIdentifiesNode.nodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.advertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange(rangeName, rangeStart, rangeEnd);
            NamedRange expectedRange2 = new NamedRange(rangeName2, rangeStart2, rangeEnd2);
            assertThat(portIdentifiesNode.nodeIdRanges()).containsExactly(expectedRange, expectedRange2);
        });
    }

    @Test
    void numIdentifyingPortsRequiredMultipleRanges() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(NODE_ID_RANGE, createNodeIdRange("a", 5L, 6L)));
        // 5 ports for the nodeId ranges + 1 for bootstrap
        assertThat(definition.numIdentifyingPortsRequired()).isEqualTo(6);
    }

    @Test
    void rangesMustBeNonEmpty() {
        List<NodeIdRanges> nodeIdRanges = List.of();
        assertThatThrownBy(() -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY, nodeIdRanges))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void proxyContainerPortsSingleRange() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 3L)));
        IngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();
        assertThat(instance.proxyContainerPorts())
                .containsExactly(createContainerPort(1 + "-bootstrap", 1),
                        createContainerPort(2 + "-node", 2),
                        createContainerPort(3 + "-node", 3),
                        createContainerPort(4 + "-node", 4));
    }

    @Test
    void proxyContainerPortsMultipleRanges() {
        ClusterIPIngressDefinition definition = new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY,
                List.of(createNodeIdRange("a", 1L, 1L), createNodeIdRange("b", 3L, 4L)));
        IngressInstance instance = definition.createInstance(1, 4);
        assertThat(instance).isNotNull();
        assertThat(instance.proxyContainerPorts())
                .containsExactly(createContainerPort(1 + "-bootstrap", 1),
                        createContainerPort(2 + "-node", 2),
                        createContainerPort(3 + "-node", 3),
                        createContainerPort(4 + "-node", 4));
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        List<NodeIdRanges> nodeIdRanges = List.of(NODE_ID_RANGE);
        ThrowingCallable nullIngress = () -> new ClusterIPIngressDefinition(null, VIRTUAL_KAFKA_CLUSTER, PROXY, nodeIdRanges);
        ThrowingCallable nullVirtualCluster = () -> new ClusterIPIngressDefinition(INGRESS, null, PROXY, nodeIdRanges);
        ThrowingCallable nullProxy = () -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, null, nodeIdRanges);
        ThrowingCallable nullRanges = () -> new ClusterIPIngressDefinition(INGRESS, VIRTUAL_KAFKA_CLUSTER, PROXY, null);
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
        Map<String, String> orderedLabels = new java.util.LinkedHashMap<>();
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

    private static ContainerPort createContainerPort(String name, int containerPort) {
        return new ContainerPortBuilder().withName(name).withContainerPort(containerPort).build();
    }

}
