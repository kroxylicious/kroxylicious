/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.Service;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancerBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.loadbalancer.Service.ExternalTrafficPolicy;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.Ingresses;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.IngressesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.resolver.ClusterResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.IngressResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ProxyResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyNetworkingModelTest {

    private static final String PROXY_NAME = "my-proxy";
    private static final String INGRESS_NAME = "my-ingress";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String NAMESPACE = "ns";

    // @formatter:off
    private static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName(PROXY_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .build();

    private static final KafkaService KAFKA_SERVICE = new KafkaServiceBuilder()
            .withNewMetadata()
                .withName("ks")
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withBootstrapServers("localhost:9092")
            .endSpec()
            .build();

    private static final Tls TLS = new TlsBuilder()
            .withNewCertificateRef()
                .withName("cert")
            .endCertificateRef()
            .build();

    private static final Ingresses CLUSTER_INGRESS = new IngressesBuilder()
            .withTls(TLS)
            .withNewIngressRef()
                .withName(INGRESS_NAME)
            .endIngressRef()
            .build();

    private static final VirtualKafkaCluster CLUSTER = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName(CLUSTER_NAME)
                .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
                .withIngresses(CLUSTER_INGRESS)
            .endSpec()
            .build();
    // @formatter:on

    private KafkaProxyIngress ingress(Boolean allocateLoadBalancerNodePorts, ExternalTrafficPolicy policy) {
        var lbBuilder = new LoadBalancerBuilder()
                .withBootstrapAddress("bootstrap.kafka")
                .withAdvertisedBrokerAddressPattern("$(nodeId).kafka");
        if (allocateLoadBalancerNodePorts != null || policy != null) {
            var svc = lbBuilder.withNewService();
            if (allocateLoadBalancerNodePorts != null) {
                svc = svc.withAllocateLoadBalancerNodePorts(allocateLoadBalancerNodePorts);
            }
            if (policy != null) {
                svc = svc.withExternalTrafficPolicy(policy);
            }
            lbBuilder = svc.endService();
        }
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withName(INGRESS_NAME)
                    .withNamespace(NAMESPACE)
                .endMetadata()
                .withNewSpec()
                    .withLoadBalancer(lbBuilder.build())
                .endSpec()
                .build();
        // @formatter:on
    }

    private Map<String, Service> services(KafkaProxyIngress ingress) {
        ClusterResolutionResult resolution = new ClusterResolutionResult(
                CLUSTER,
                ResolutionResult.resolved(CLUSTER, PROXY),
                List.of(),
                ResolutionResult.resolved(CLUSTER, KAFKA_SERVICE),
                List.of(new IngressResolutionResult(
                        ResolutionResult.resolved(CLUSTER, ingress),
                        ResolutionResult.resolved(ingress, PROXY),
                        CLUSTER_INGRESS)));
        ProxyResolutionResult proxyResolution = new ProxyResolutionResult(Set.of(resolution));
        ProxyNetworkingModel networkingModel = NetworkingPlanner.planNetworking(PROXY, proxyResolution, List.of());
        return networkingModel.services(PROXY, cluster -> true)
                .collect(Collectors.toMap(s -> s.getMetadata().getName(), s -> s));
    }

    // --- nodePort emission cases ---

    @Test
    void shouldEmitNodePortZeroOnEveryPortWhenAllocateLoadBalancerNodePortsFalse() {
        // Given

        // When
        var result = services(ingress(false, null));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getPorts())
                .singleElement()
                .satisfies(p -> {
                    assertThat(p.getNodePort()).isZero();
                    assertThat(p.getName()).isEqualTo("sni-9083");
                    assertThat(p.getPort()).isEqualTo(9083);
                    assertThat(p.getProtocol()).isEqualTo("TCP");
                });
    }

    @Test
    void shouldNotEmitNodePortWhenAllocateLoadBalancerNodePortsTrue() {
        // Given

        // When
        var result = services(ingress(true, null));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getPorts())
                .singleElement()
                .satisfies(p -> assertThat(p.getNodePort()).isNull());
    }

    @Test
    void shouldNotEmitNodePortWhenAllocateLoadBalancerNodePortsUnset() {
        // Given

        // When
        var result = services(ingress(null, null));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getPorts())
                .singleElement()
                .satisfies(p -> assertThat(p.getNodePort()).isNull());
    }

    // --- spec field propagation cases ---

    @Test
    void shouldCarryExternalTrafficPolicyLocalOnDesiredService() {
        // Given

        // When
        var result = services(ingress(null, ExternalTrafficPolicy.LOCAL));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getExternalTrafficPolicy()).isEqualTo("Local");
    }

    @Test
    void shouldCarryExternalTrafficPolicyClusterOnDesiredService() {
        // Given

        // When
        var result = services(ingress(null, ExternalTrafficPolicy.CLUSTER));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getExternalTrafficPolicy()).isEqualTo("Cluster");
    }

    @Test
    void shouldNotEmitDefaultWhenNeitherServiceFieldSet() {
        // Given

        // When
        var result = services(ingress(null, null));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getExternalTrafficPolicy()).isNull();
        assertThat(result.get(INGRESS_NAME).getSpec().getAllocateLoadBalancerNodePorts()).isNull();
    }

    @Test
    void shouldCarryAllocateLoadBalancerNodePortsFalseOnDesiredService() {
        // Given

        // When
        var result = services(ingress(false, null));

        // Then
        assertThat(result.get(INGRESS_NAME).getSpec().getAllocateLoadBalancerNodePorts()).isFalse();
    }
}
