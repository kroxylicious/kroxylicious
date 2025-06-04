/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Set;

import org.assertj.core.api.ListAssert;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.ClusterIP;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancerBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.IngressResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import static org.assertj.core.api.Assertions.assertThat;

class NetworkingPlannerTest {
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
                    .withProtocol(ClusterIP.Protocol.TCP)
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
                    .withProtocol(ClusterIP.Protocol.TLS)
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
                    .withProtocol(ClusterIP.Protocol.TLS)
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
                    .withProtocol(ClusterIP.Protocol.TCP)
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
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of()));
        assertThat(networkingModel.clusterNetworkingModels()).isEmpty();
    }

    @Test
    void clusterIpIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, CLUSTER_IP_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, CLUSTER_IP_INGRESS),
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
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
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS, 9291));
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
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS, 9291));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel())
                        .isEqualTo(new TlsClusterIPClusterIngressNetworkingModel(PROXY, virtualKafkaCluster, TLS_CLUSTER_IP_2_INGRESS,
                                List.of(new NodeIdRangesBuilder().withName("default").withStart(0L).withEnd(2L).build()), TLS_2, 9291));
            });
        });
    }

    @Test
    void loadBalancerIngress() {
        VirtualKafkaCluster virtualKafkaCluster = clusterWithIngress(CLUSTER_NAME, LOAD_BALANCER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult = new ClusterResolutionResult(virtualKafkaCluster, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster, LOAD_BALANCER_INGRESS),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, 9291));
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
                                ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
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
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, 9291));
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
                                ResolutionResult.resolved(LOAD_BALANCER_INGRESS_2, PROXY), LOAD_BALANCER_2_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        assertThat(networkingModel.clusterNetworkingModels()).singleElement().satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            var listAssert = assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).hasSize(2);
            listAssert.element(0).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, 9291));
            });
            listAssert.element(1).satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS_2, LOAD_BALANCER_2, TLS_2, 9291));
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
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS, PROXY), LOAD_BALANCER_INGRESSES)),
                List.of());
        ClusterResolutionResult clusterResolutionResult2 = new ClusterResolutionResult(virtualKafkaCluster2, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster2, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster2, LOAD_BALANCER_INGRESS_2),
                        ResolutionResult.resolved(LOAD_BALANCER_INGRESS_2, PROXY), LOAD_BALANCER_2_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY,
                new ProxyResolutionResult(Set.of(clusterResolutionResult, clusterResolutionResult2)));
        assertThat(networkingModel.clusterIngressModel(virtualKafkaCluster)).isNotNull();

        ListAssert<ProxyNetworkingModel.ClusterNetworkingModel> listAssert = assertThat(networkingModel.clusterNetworkingModels()).hasSize(2);
        listAssert.element(0).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster, LOAD_BALANCER_INGRESS, LOAD_BALANCER, TLS, 9291));

            });
        });
        listAssert.element(1).satisfies(clusterNetworkingModel -> {
            assertThat(clusterNetworkingModel.cluster()).isEqualTo(virtualKafkaCluster2);
            assertThat(clusterNetworkingModel.ingressExceptions()).isEmpty();
            assertThat(clusterNetworkingModel.clusterIngressNetworkingModelResults()).singleElement().satisfies(model -> {
                assertThat(model.exception()).isNull();
                assertThat(model.clusterIngressNetworkingModel()).isEqualTo(
                        new LoadBalancerClusterIngressNetworkingModel(virtualKafkaCluster2, LOAD_BALANCER_INGRESS_2, LOAD_BALANCER_2, TLS_2, 9291));

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
                                ResolutionResult.resolved(CLUSTER_IP_INGRESS_2, PROXY), CLUSTER_IP_2_CLUSTER_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, new ProxyResolutionResult(Set.of(clusterResolutionResult)));
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
                        ResolutionResult.resolved(CLUSTER_IP_INGRESS, PROXY), CLUSTER_IP_CLUSTER_INGRESSES)),
                List.of());

        VirtualKafkaCluster virtualKafkaCluster2 = clusterWithIngress(CLUSTER_NAME_2, CLUSTER_IP_2_CLUSTER_INGRESSES);
        ClusterResolutionResult clusterResolutionResult2 = new ClusterResolutionResult(virtualKafkaCluster2, ResolutionResult.resolved(virtualKafkaCluster, PROXY),
                List.of(), ResolutionResult.resolved(virtualKafkaCluster2, KAFKA_SERVICE),
                List.of(
                        new IngressResolutionResult(ResolutionResult.resolved(virtualKafkaCluster2, CLUSTER_IP_INGRESS_2),
                                ResolutionResult.resolved(CLUSTER_IP_INGRESS_2, PROXY), CLUSTER_IP_2_CLUSTER_INGRESSES)),
                List.of());
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY,
                new ProxyResolutionResult(Set.of(clusterResolutionResult, clusterResolutionResult2)));
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

}