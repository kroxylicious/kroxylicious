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

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ContainerPortBuilder;
import io.fabric8.kubernetes.api.model.IntOrString;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;

import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.proxy.config.NamedRange;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class TcpClusterIPClusterIngressNetworkingModelTest {

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
                    .withProtocol(Protocol.TCP)
                .endClusterIP()
            .endSpec()
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
        // 3 ports for the nodeId range + 1 for bootstrap
        int numIdentifyingPortsRequired = TcpClusterIPClusterIngressNetworkingModel.numIdentifyingPortsRequired(List.of(NODE_ID_RANGE));
        assertThat(numIdentifyingPortsRequired).isEqualTo(4);
    }

    @Test
    void createInstancesWithExpectedNumber() {
        // given
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange("a", 1L, 3L));
        // when
        TcpClusterIPClusterIngressNetworkingModel model = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, 1, 4);
        // then
        assertThat(model).isNotNull();
    }

    @Test
    void createInstancesWithTooManyPorts() {
        // given
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange("a", 1L, 3L));
        // when
        // then
        // more ports than the 4 required by the node id range + 1 for bootstrap
        assertThatThrownBy(() -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, 1, 5)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void createInstancesWithNotEnoughPorts() {
        // given
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange("a", 1L, 3L));

        // when
        // then
        // more ports than the 4 required by the node id range + 1 for bootstrap
        assertThatThrownBy(() -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, 1, 3)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void gatewayConfigSingleRange() {
        // given
        String rangeName = "a";
        int rangeStart = 1;
        int rangeEnd = 3;
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd));
        // 3 ports for the nodeId range + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, 1, 4);
        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.getBootstrapAddress()).isEqualTo(new HostPort("localhost", 1));
            assertThat(portIdentifiesNode.getNodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.getAdvertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange(rangeName, rangeStart, rangeEnd);
            assertThat(portIdentifiesNode.getNodeIdRanges()).containsExactly(expectedRange);
        });
    }

    @Test
    void serviceMetadata() {
        // given
        // 3 ports for the nodeId range + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), 1, 4);
        assertThat(instance).isNotNull();

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            Map<String, String> orderedServiceLabels = commonLabels(PROXY_NAME);
            assertThat(build.getMetadata()).isNotNull().satisfies(metadata -> {
                assertThat(metadata.getNamespace()).isEqualTo(NAMESPACE);
                assertThat(metadata.getName()).isEqualTo(CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap");
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
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), 1, 4);

        // when
        List<ServiceBuilder> serviceBuilders = instance.services().toList();

        // then
        assertThat(serviceBuilders).isNotNull().isNotEmpty().singleElement().satisfies(serviceBuild -> {
            Service build = serviceBuild.build();
            assertThat(build.getSpec()).isNotNull().satisfies(serviceSpec -> {
                LinkedHashMap<String, String> orderedSelectorLabels = new LinkedHashMap<>(commonLabels(PROXY_NAME));
                assertThat(serviceSpec.getSelector()).containsExactlyEntriesOf(orderedSelectorLabels);
            });
        });
    }

    @Test
    void servicesSingleRangePorts() {
        // given
        // 3 ports for the nodeId range + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), 1, 4);

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
        // 2 ports for the nodeId ranges + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 1), createNodeIdRange("b", 3, 3)), 1, 3);

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
        List<NodeIdRanges> nodeIdRange = List.of(createNodeIdRange(null, rangeStart, rangeEnd), createNodeIdRange(null, rangeStart2, rangeEnd2));

        // 5 ports for the nodeId ranges + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                nodeIdRange, 2, 7);

        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.getBootstrapAddress()).isEqualTo(new HostPort("localhost", 2));
            assertThat(portIdentifiesNode.getNodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.getAdvertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange("range-0", rangeStart, rangeEnd);
            NamedRange expectedRange2 = new NamedRange("range-1", rangeStart2, rangeEnd2);
            assertThat(portIdentifiesNode.getNodeIdRanges()).containsExactly(expectedRange, expectedRange2);
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
        // 5 ports for the nodeId ranges + 1 for bootstrap
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange(rangeName, rangeStart, rangeEnd), createNodeIdRange(rangeName2, rangeStart2, rangeEnd2)), 2, 7);

        assertThat(instance).isNotNull();

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isEmpty();
        assertThat(gateway.sniHostIdentifiesNode()).isNull();
        assertThat(gateway.portIdentifiesNode()).isNotNull().satisfies(portIdentifiesNode -> {
            assertThat(portIdentifiesNode.getBootstrapAddress()).isEqualTo(new HostPort("localhost", 2));
            assertThat(portIdentifiesNode.getNodeStartPort()).isNull(); // we use the default of bootstrap port + 1
            String expectedAdvertisedAddress = CLUSTER_NAME + "-" + INGRESS_NAME + "-bootstrap." + NAMESPACE + ".svc.cluster.local";
            assertThat(portIdentifiesNode.getAdvertisedBrokerAddressPattern()).isEqualTo(expectedAdvertisedAddress);
            NamedRange expectedRange = new NamedRange(rangeName, rangeStart, rangeEnd);
            NamedRange expectedRange2 = new NamedRange(rangeName2, rangeStart2, rangeEnd2);
            assertThat(portIdentifiesNode.getNodeIdRanges()).containsExactly(expectedRange, expectedRange2);
        });
    }

    @Test
    void numIdentifyingPortsRequiredMultipleRanges() {
        // 5 ports for the nodeId ranges + 1 for bootstrap
        assertThat(TcpClusterIPClusterIngressNetworkingModel.numIdentifyingPortsRequired(List.of(NODE_ID_RANGE, createNodeIdRange("a", 5L, 6L)))).isEqualTo(6);
    }

    @Test
    void rangesMustBeNonEmpty() {
        List<NodeIdRanges> nodeIdRanges = List.of();
        assertThatThrownBy(() -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, nodeIdRanges, 1, 3))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void identifyingProxyContainerPortsSingleRange() {
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1, 3)), 1, 4);

        assertThat(instance).isNotNull();
        assertThat(instance.identifyingProxyContainerPorts())
                .containsExactly(createContainerPort(1 + "-bootstrap", 1),
                        createContainerPort(2 + "-node", 2),
                        createContainerPort(3 + "-node", 3),
                        createContainerPort(4 + "-node", 4));
    }

    @Test
    void identifyingProxyContainerPortsMultipleRanges() {
        ClusterIngressNetworkingModel instance = new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS,
                List.of(createNodeIdRange("a", 1L, 1L), createNodeIdRange("b", 3L, 4L)), 1, 4);

        assertThat(instance).isNotNull();
        assertThat(instance.identifyingProxyContainerPorts())
                .containsExactly(createContainerPort(1 + "-bootstrap", 1),
                        createContainerPort(2 + "-node", 2),
                        createContainerPort(3 + "-node", 3),
                        createContainerPort(4 + "-node", 4));
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        List<NodeIdRanges> nodeIdRanges = List.of(NODE_ID_RANGE);
        ThrowingCallable nullIngress = () -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, null, nodeIdRanges, 1, 4);
        ThrowingCallable nullVirtualCluster = () -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, null, INGRESS, nodeIdRanges, 1, 4);
        ThrowingCallable nullProxy = () -> new TcpClusterIPClusterIngressNetworkingModel(null, VIRTUAL_KAFKA_CLUSTER, INGRESS, nodeIdRanges, 1, 4);
        ThrowingCallable nullRanges = () -> new TcpClusterIPClusterIngressNetworkingModel(PROXY, VIRTUAL_KAFKA_CLUSTER, INGRESS, null, 1, 4);
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
        orderedLabels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        orderedLabels.put("app.kubernetes.io/name", "kroxylicious");
        orderedLabels.put("app.kubernetes.io/component", "proxy");
        orderedLabels.put("app.kubernetes.io/instance", proxyName);
        return orderedLabels;
    }

    private static NodeIdRanges createNodeIdRange(@Nullable String name, long start, long endInclusive) {
        return new NodeIdRangesBuilder().withName(name).withStart(start).withEnd(endInclusive).build();
    }

    private static ContainerPort createContainerPort(String name, int containerPort) {
        return new ContainerPortBuilder().withName(name).withContainerPort(containerPort).build();
    }

}
