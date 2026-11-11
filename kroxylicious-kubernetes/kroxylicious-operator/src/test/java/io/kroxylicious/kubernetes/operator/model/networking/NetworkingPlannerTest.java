/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.kroxylicious.kubernetes.api.common.Protocol;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancerBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.loadbalancer.ProxyProtocol;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.model.RouteHostDetails;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.IngressResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;

class NetworkingPlannerTest {
    private static final SharedSniContainerPort PLAIN_SNI_PORT = new SharedSniContainerPort(9291, "shared-sni-port");
    private static final SharedSniContainerPort PROXY_PROTOCOL_SNI_PORT = new SharedSniContainerPort(9290, "shared-sni-proxy-protocol-port");

    private static final String PROXY_NAME = "my-proxy";
    private static final String CLUSTER_IP_INGRESS_NAME = "my-cluster-ip-ingress";
    private static final String TLS_CLUSTER_IP_INGRESS_NAME = "my-tls-cluster-ip-ingress";
    private static final String TLS_CLUSTER_IP_2_INGRESS_NAME = "my-tls-cluster-ip-ingress-2";
    private static final String CLUSTER_IP_2_INGRESS_NAME = "my-cluster-ip-ingress-2";
    private static final String LOAD_BALANCER_INGRESS_NAME = "my-load-balancer-ingress";
    private static final String LOAD_BALANCER_INGRESS_2_NAME = "my-load-balancer-ingress-2";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String KAFKA_SERVICE_NAME = "my-kafka-service";
    private static final String NAMESPACE = "ns";
    private static final List<RouteHostDetails> ROUTE_HOST_DETAILS = List.of();
    // @formatter:off
    private static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName(PROXY_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();
    // @formatter:on

    // @formatter:off
    private static final KafkaProxyIngress CLUSTER_IP_INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(CLUSTER_IP_INGRESS_NAME)
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
    private static final KafkaProxyIngress TLS_CLUSTER_IP_INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(TLS_CLUSTER_IP_INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewClusterIP()
                    .withProtocol(Protocol.TLS)
                .endClusterIP()
            .endSpec()
            .build();
    // @formatter:on

    // @formatter:off
    private static final KafkaProxyIngress TLS_CLUSTER_IP_2_INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(TLS_CLUSTER_IP_2_INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewClusterIP()
                    .withProtocol(Protocol.TLS)
                .endClusterIP()
            .endSpec()
            .build();
    // @formatter:on

    // @formatter:off
    private static final KafkaProxyIngress CLUSTER_IP_INGRESS_2 = new KafkaProxyIngressBuilder()
            .withNewMetadata()
                .withName(CLUSTER_IP_2_INGRESS_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withNewClusterIP()
                    .withProtocol(Protocol.TCP)
                .endClusterIP()
            .endSpec()
            .build();
    // @formatter:on

    private static final Tls TLS = new TlsBuilder().withNewCertificateRef().withName("server-cert").endCertificateRef().build();
    private static final Tls TLS_2 = new TlsBuilder().withNewCertificateRef().withName("server-cert2").endCertificateRef().build();

    public static final LoadBalancer LOAD_BALANCER = new LoadBalancerBuilder()
            .withBootstrapAddress("bootstrap.kafka")
            .withAdvertisedBrokerAddressPattern("$(nodeId)-broker.kafka")
            .build();
    private static final KafkaProxyIngress LOAD_BALANCER_INGRESS = createLoadBalancerIngress(LOAD_BALANCER_INGRESS_NAME, LOAD_BALANCER);
    public static final LoadBalancer LOAD_BALANCER_2 = new LoadBalancerBuilder()
            .withBootstrapAddress("bootstrap.other")
            .withAdvertisedBrokerAddressPattern("$(nodeId)-broker.other")
            .build();
    private static final KafkaProxyIngress LOAD_BALANCER_INGRESS_2 = createLoadBalancerIngress(LOAD_BALANCER_INGRESS_2_NAME, LOAD_BALANCER_2);

    private static KafkaProxyIngress createLoadBalancerIngress(String name, LoadBalancer loadBalancer) {
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                .withName(name)
                .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                .withLoadBalancer(loadBalancer)
                .endSpec()
                .build();
    }
    // @formatter:on

    public static final Ingresses CLUSTER_IP_CLUSTER_INGRESSES = new IngressesBuilder().withNewIngressRef().withName(CLUSTER_IP_INGRESS_NAME).endIngressRef().build();
    public static final Ingresses TLS_CLUSTER_IP_CLUSTER_INGRESSES = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName(TLS_CLUSTER_IP_INGRESS_NAME)
            .endIngressRef().build();
    public static final Ingresses TLS_CLUSTER_IP_CLUSTER_2_INGRESSES = new IngressesBuilder().withTls(TLS_2).withNewIngressRef().withName(TLS_CLUSTER_IP_2_INGRESS_NAME)
            .endIngressRef().build();

    public static final Ingresses CLUSTER_IP_2_CLUSTER_INGRESSES = new IngressesBuilder().withNewIngressRef().withName(CLUSTER_IP_2_INGRESS_NAME).endIngressRef().build();
    public static final Ingresses LOAD_BALANCER_INGRESSES = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName(LOAD_BALANCER_INGRESS_NAME).endIngressRef()
            .build();
    public static final Ingresses LOAD_BALANCER_2_INGRESSES = new IngressesBuilder().withTls(TLS_2).withNewIngressRef().withName(LOAD_BALANCER_INGRESS_2_NAME)
            .endIngressRef().build();

    public static final int FIRST_IDENTIFYING_PORT = 9292;
    public static final String CLUSTER_NAME_2 = "my-cluster-2";

    // @formatter:off
    private static VirtualKafkaCluster clusterWithIngress(String clusterName, Ingresses... ingresses) {
        return new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName(clusterName)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withIngresses(ingresses)
                .endSpec()
            .build();
    }
    // @formatter:on

    // @formatter:off
    private static final KafkaService KAFKA_SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName(KAFKA_SERVICE_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withBootstrapServers("localhost:9092")
            .endSpec()
            .build();
    // @formatter:on

    @Test
    void noClustersResolved() {
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of()), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterNetworkingModels()).isEmpty();
    }

    @Test
    void clusterIpIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, CLUSTER_IP_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, CLUSTER_IP_INGRESS,
                        List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT,
                        FIRST_IDENTIFYING_PORT + 3));
            });
        });
    }

    @Test
    void tlsClusterIpIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, TLS_CLUSTER_IP_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(TLS_CLUSTER_IP_INGRESS, PROXY), TLS_CLUSTER_IP_CLUSTER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS, PLAIN_SNI_PORT));
            });
        });
    }

    @Test
    void mulipleTlsClusterIpIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, TLS_CLUSTER_IP_CLUSTER_INGRESSES, TLS_CLUSTER_IP_CLUSTER_2_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(TLS_CLUSTER_IP_INGRESS, PROXY), TLS_CLUSTER_IP_CLUSTER_INGRESSES),
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, TLS_CLUSTER_IP_2_INGRESS),
                                ResolutionResult.resolved(TLS_CLUSTER_IP_2_INGRESS, PROXY), TLS_CLUSTER_IP_CLUSTER_2_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS, PLAIN_SNI_PORT));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_2_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS_2, PLAIN_SNI_PORT));
            });
        });
    }

    @Test
    void loadBalancerIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, LOAD_BALANCER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, PLAIN_SNI_PORT));
            });
        });
    }

    @Test
    void clusterIpAndLoadBalancerIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, CLUSTER_IP_CLUSTER_INGRESSES, LOAD_BALANCER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES),
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS),
                                ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, CLUSTER_IP_INGRESS,
                        List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT,
                        FIRST_IDENTIFYING_PORT + 3));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, PLAIN_SNI_PORT));
            });

        });
    }

    @Test
    void multipleLoadBalancerIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, LOAD_BALANCER_INGRESSES, LOAD_BALANCER_2_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES),
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS_2),
                                ResolutionResult.resolved(LOAD_BALANCER_INGRESS_2, PROXY), LOAD_BALANCER_2_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, PLAIN_SNI_PORT));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS_2, LOAD_BALANCER_2, TLS_2, PLAIN_SNI_PORT));
            });
        });
    }

    @Test
    void multipleClustersWithLoadBalancerIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, LOAD_BALANCER_INGRESSES);
        VirtualKafkaCluster virtualKafkaCluster2 = clusterWithIngress(CLUSTER_NAME_2, LOAD_BALANCER_2_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)));
        ClusterResolutionResult clusterResolutionResult2 = new ClusterResolutionResult(virtualKafkaCluster2, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster2, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster2, LOAD_BALANCER_INGRESS_2),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS_2, PROXY), LOAD_BALANCER_2_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY,
                new ProxyResolutionResult(Set.of(clusterResolutionResult, clusterResolutionResult2)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        ListAssert<ProxyNetworkingModel.ClusterNetworkingModel> listAssert = assertThat(networkingModel.clusterNetworkingModels()).hasSize(2);
        listAssert.element(0).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, PLAIN_SNI_PORT));

            });
        });
        listAssert.element(1).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster2);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster2, LOAD_BALANCER_INGRESS_2, LOAD_BALANCER_2, TLS_2, PLAIN_SNI_PORT));

            });
        });
    }

    @Test
    void multipleClusterIpIngresses() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, CLUSTER_IP_CLUSTER_INGRESSES, CLUSTER_IP_2_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES),
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS_2),
                                ResolutionResult.resolved(CLUSTER_IP_INGRESS_2, PROXY), CLUSTER_IP_2_CLUSTER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).hasSize(1).singleElement().satisfies(model -> {
                assertThat(model).hasMessageContaining("Currently we do not support a virtual cluster with multiple ingresses that need unique ports to"
                        + " identify which node the client is connecting to");
            });
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, CLUSTER_IP_INGRESS,
                        List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT,
                        FIRST_IDENTIFYING_PORT + 3));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNotNull().hasMessageContaining(
                        "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to"
                                + " identify which node the client is connecting to");
                // ports are still allocated to try and keep the model stable
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, CLUSTER_IP_INGRESS_2,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT + 4,
                                FIRST_IDENTIFYING_PORT + 7));
            });
        });
    }

    @Test
    void multipleClustersWithClusterIpIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, CLUSTER_IP_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES)));

        VirtualKafkaCluster virtualKafkaCluster2 = clusterWithIngress(CLUSTER_NAME_2, CLUSTER_IP_2_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult2 = new ClusterResolutionResult(virtualKafkaCluster2, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster2, KAFKA_SERVICE),
                List.of(
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster2, CLUSTER_IP_INGRESS_2),
                                ResolutionResult.resolved(CLUSTER_IP_INGRESS_2, PROXY), CLUSTER_IP_2_CLUSTER_INGRESSES)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY,
                new ProxyResolutionResult(Set.of(clusterResolutionResult, clusterResolutionResult2)), ROUTE_HOST_DETAILS);
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster2)).isNotNull();

        var assertList = assertThat(networkingModel.clusterNetworkingModels()).hasSize(2);
        assertList.element(0).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, CLUSTER_IP_INGRESS,
                        List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT,
                        FIRST_IDENTIFYING_PORT + 3));

            });
        });
        assertList.element(1).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster2);
            assertThat(clusterNetworkingModel.ingressExceptions()).singleElement().satisfies(ingressConflictException -> {
                assertThat(ingressConflictException).hasMessageContaining(
                        "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to"
                                + " identify which node the client is connecting to");
            });
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNotNull().hasMessageContaining(
                        "Currently we do not support a virtual cluster with multiple ingresses that need unique ports to"
                                + " identify which node the client is connecting to");
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TcpClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster2, CLUSTER_IP_INGRESS_2,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), FIRST_IDENTIFYING_PORT + 4,
                                FIRST_IDENTIFYING_PORT + 7));

            });
        });
    }

    static Stream<Arguments> loadBalancerProxyProtocolModeProvider() {
        return Stream.of(
                Arguments.of(ProxyProtocol.Mode.REQUIRED, PROXY_PROTOCOL_SNI_PORT),
                Arguments.of(ProxyProtocol.Mode.FORBIDDEN, PLAIN_SNI_PORT),
                Arguments.of(null, PLAIN_SNI_PORT));
    }

    @Test
    void loadBalancerProxyProtocolPresentButModeNullDefaultsToForbiddenPort() {
        // Given
        LoadBalancer loadBalancer = new LoadBalancerBuilder()
                .withBootstrapAddress("bootstrap.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-broker.kafka")
                .withNewProxyProtocol()
                .endProxyProtocol()
                .build();
        String ingressName = "lb-no-mode";
        KafkaProxyIngress ingress = createLoadBalancerIngress(ingressName, loadBalancer);
        Ingresses ingresses = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName(ingressName).endIngressRef().build();
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, ingresses);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, ingress),
                        ResolutionResult.resolved(ingress, PROXY), ingresses)));

        // When
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);

        // Then
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> true)).containsExactly(PLAIN_SNI_PORT);
    }

    @ParameterizedTest
    @MethodSource("loadBalancerProxyProtocolModeProvider")
    void loadBalancerProxyProtocolModeUsesCorrectPort(@Nullable ProxyProtocol.Mode mode,
                                                      SharedSniContainerPort expectedPort) {
        // Given
        LoadBalancerBuilder lbBuilder = new LoadBalancerBuilder()
                .withBootstrapAddress("bootstrap.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-broker.kafka");
        if (mode != null) {
            lbBuilder.withNewProxyProtocol().withMode(mode).endProxyProtocol();
        }
        LoadBalancer loadBalancer = lbBuilder.build();
        String ingressName = "lb-" + (mode != null ? mode.name().toLowerCase() : "default");
        KafkaProxyIngress ingress = createLoadBalancerIngress(ingressName, loadBalancer);
        Ingresses ingresses = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName(ingressName).endIngressRef().build();
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, ingresses);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, ingress),
                        ResolutionResult.resolved(ingress, PROXY), ingresses)));

        // When
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);

        // Then
        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, ingress, loadBalancer, TLS, expectedPort));
            });
        });
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> true)).containsExactly(expectedPort);
    }

    @Test
    void multipleLoadBalancersWithDifferentProxyProtocolModesUseDifferentPorts() {
        // Given
        LoadBalancer lbRequired = new LoadBalancerBuilder()
                .withBootstrapAddress("required.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-required.kafka")
                .withNewProxyProtocol()
                .withMode(ProxyProtocol.Mode.REQUIRED)
                .endProxyProtocol()
                .build();
        KafkaProxyIngress ingressRequired = createLoadBalancerIngress("lb-required", lbRequired);
        Ingresses ingressesRequired = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName("lb-required").endIngressRef().build();

        LoadBalancer lbForbidden = new LoadBalancerBuilder()
                .withBootstrapAddress("forbidden.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-forbidden.kafka")
                .withNewProxyProtocol()
                .withMode(ProxyProtocol.Mode.FORBIDDEN)
                .endProxyProtocol()
                .build();
        KafkaProxyIngress ingressForbidden = createLoadBalancerIngress("lb-forbidden", lbForbidden);
        Ingresses ingressesForbidden = new IngressesBuilder().withTls(TLS_2).withNewIngressRef().withName("lb-forbidden").endIngressRef().build();

        VirtualKafkaCluster cluster1 = clusterWithIngress(CLUSTER_NAME, ingressesRequired);
        VirtualKafkaCluster cluster2 = clusterWithIngress(CLUSTER_NAME_2, ingressesForbidden);

        ClusterResolutionResult clusterResult1 = new ClusterResolutionResult(cluster1, ResolutionResult.resolved(cluster1, PROXY),
                List.of(), ResolutionResult.resolved(cluster1, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(cluster1, ingressRequired),
                        ResolutionResult.resolved(ingressRequired, PROXY), ingressesRequired)));

        ClusterResolutionResult clusterResult2 = new ClusterResolutionResult(cluster2, ResolutionResult.resolved(cluster2, PROXY),
                List.of(), ResolutionResult.resolved(cluster2, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(cluster2, ingressForbidden),
                        ResolutionResult.resolved(ingressForbidden, PROXY), ingressesForbidden)));

        // When
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResult1, clusterResult2)),
                ROUTE_HOST_DETAILS);

        // Then
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> true)).containsExactly(PROXY_PROTOCOL_SNI_PORT, PLAIN_SNI_PORT);
        assertThat(networkingModel.clusterIngressModel(cluster1)).isPresent().get().satisfies(cluster1Model -> {
            assertThat(cluster1Model.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(cluster1, ingressRequired, lbRequired, TLS, PROXY_PROTOCOL_SNI_PORT));
            });
        });
        assertThat(networkingModel.clusterIngressModel(cluster2)).isPresent().get().satisfies(cluster2Model -> {
            assertThat(cluster2Model.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(cluster2, ingressForbidden, lbForbidden, TLS_2, PLAIN_SNI_PORT));
            });
        });
    }

    @Test
    void multipleLoadBalancersWithSameProxyProtocolModeShareSinglePort() {
        // Given
        LoadBalancer lbRequiredOne = new LoadBalancerBuilder()
                .withBootstrapAddress("required-one.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-required-one.kafka")
                .withNewProxyProtocol()
                .withMode(ProxyProtocol.Mode.REQUIRED)
                .endProxyProtocol()
                .build();
        KafkaProxyIngress ingressRequiredOne = createLoadBalancerIngress("lb-required-one", lbRequiredOne);
        Ingresses ingressesRequiredOne = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName("lb-required-one").endIngressRef().build();

        LoadBalancer lbRequiredTwo = new LoadBalancerBuilder()
                .withBootstrapAddress("required-two.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-required-two.kafka")
                .withNewProxyProtocol()
                .withMode(ProxyProtocol.Mode.REQUIRED)
                .endProxyProtocol()
                .build();
        KafkaProxyIngress ingressRequiredTwo = createLoadBalancerIngress("lb-required-two", lbRequiredTwo);
        Ingresses ingressesRequiredTwo = new IngressesBuilder().withTls(TLS_2).withNewIngressRef().withName("lb-required-two").endIngressRef().build();

        VirtualKafkaCluster cluster1 = clusterWithIngress(CLUSTER_NAME, ingressesRequiredOne);
        VirtualKafkaCluster cluster2 = clusterWithIngress(CLUSTER_NAME_2, ingressesRequiredTwo);

        ClusterResolutionResult clusterResult1 = new ClusterResolutionResult(cluster1, ResolutionResult.resolved(cluster1, PROXY),
                List.of(), ResolutionResult.resolved(cluster1, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(cluster1, ingressRequiredOne),
                        ResolutionResult.resolved(ingressRequiredOne, PROXY), ingressesRequiredOne)));

        ClusterResolutionResult clusterResult2 = new ClusterResolutionResult(cluster2, ResolutionResult.resolved(cluster2, PROXY),
                List.of(), ResolutionResult.resolved(cluster2, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(cluster2, ingressRequiredTwo),
                        ResolutionResult.resolved(ingressRequiredTwo, PROXY), ingressesRequiredTwo)));

        // When
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResult1, clusterResult2)),
                ROUTE_HOST_DETAILS);

        // Then
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> true)).containsExactly(PROXY_PROTOCOL_SNI_PORT);
    }

    @Test
    void tlsClusterIpAlwaysUses9291Port() {
        // Given
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, TLS_CLUSTER_IP_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(TLS_CLUSTER_IP_INGRESS, PROXY), TLS_CLUSTER_IP_CLUSTER_INGRESSES)));

        // When
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)), ROUTE_HOST_DETAILS);

        // Then
        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS, PLAIN_SNI_PORT));
            });
        });
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> true)).containsExactly(PLAIN_SNI_PORT);
    }

    @Test
    void requiredSharedSniPortsExcludesBrokenClusters() {
        // Given
        LoadBalancer lbRequired = new LoadBalancerBuilder()
                .withBootstrapAddress("required.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-required.kafka")
                .withNewProxyProtocol().withMode(ProxyProtocol.Mode.REQUIRED).endProxyProtocol()
                .build();
        KafkaProxyIngress ingressRequired = createLoadBalancerIngress("lb-required", lbRequired);
        Ingresses ingressesRequired = new IngressesBuilder().withTls(TLS).withNewIngressRef().withName("lb-required").endIngressRef().build();
        VirtualKafkaCluster healthyCluster = clusterWithIngress(CLUSTER_NAME, ingressesRequired);

        LoadBalancer lbForbidden = new LoadBalancerBuilder()
                .withBootstrapAddress("forbidden.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId)-forbidden.kafka")
                .withNewProxyProtocol().withMode(ProxyProtocol.Mode.FORBIDDEN).endProxyProtocol()
                .build();
        KafkaProxyIngress ingressForbidden = createLoadBalancerIngress("lb-forbidden", lbForbidden);
        Ingresses ingressesForbidden = new IngressesBuilder().withTls(TLS_2).withNewIngressRef().withName("lb-forbidden").endIngressRef().build();
        VirtualKafkaCluster brokenCluster = clusterWithIngress(CLUSTER_NAME_2, ingressesForbidden);

        ClusterResolutionResult clusterResult1 = new ClusterResolutionResult(healthyCluster, ResolutionResult.resolved(healthyCluster, PROXY),
                List.of(), ResolutionResult.resolved(healthyCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(healthyCluster, ingressRequired),
                        ResolutionResult.resolved(ingressRequired, PROXY), ingressesRequired)));
        ClusterResolutionResult clusterResult2 = new ClusterResolutionResult(brokenCluster, ResolutionResult.resolved(brokenCluster, PROXY),
                List.of(), ResolutionResult.resolved(brokenCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(brokenCluster, ingressForbidden),
                        ResolutionResult.resolved(ingressForbidden, PROXY), ingressesForbidden)));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResult1, clusterResult2)),
                ROUTE_HOST_DETAILS);

        // When / Then
        assertThat(networkingModel.requiredSharedSniPorts(cluster -> cluster.equals(healthyCluster))).containsExactly(PROXY_PROTOCOL_SNI_PORT);
    }

}