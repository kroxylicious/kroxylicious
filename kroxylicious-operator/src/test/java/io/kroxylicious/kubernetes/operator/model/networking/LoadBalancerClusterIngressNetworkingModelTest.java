/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancerBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import static io.kroxylicious.kubernetes.operator.model.networking.LoadBalancerClusterIngressNetworkingModel.DEFAULT_CLIENT_FACING_LOADBALANCER_PORT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class LoadBalancerClusterIngressNetworkingModelTest {
    private static final String INGRESS_NAME = "my-ingress";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String NAMESPACE = "my-namespace";

    private static final String BOOTSTRAP_ADDRESS = "$(virtualClusterName).kafkaproxy";
    private static final String ADVERTISED_BROKER_ADDRESS_PATTERN = "$(virtualClusterName)-$(nodeId).kafkaproxy";

    private static final LoadBalancer LOAD_BALANCER = new LoadBalancerBuilder()
            .withBootstrapAddress(BOOTSTRAP_ADDRESS)
            .withAdvertisedBrokerAddressPattern(ADVERTISED_BROKER_ADDRESS_PATTERN)
            .build();

    private static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder()
            .withNewMetadata()
            .withName(INGRESS_NAME)
            .withNamespace(NAMESPACE)
            .endMetadata()
            .withNewSpec()
            .withLoadBalancer(LOAD_BALANCER)
            .endSpec()
            .build();
    // @formatter:on

    private static final Tls TLS = new TlsBuilder().withNewCertificateRef().withName("server-cert").endCertificateRef().build();

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

    @Test
    void createInstancesWithExpectedSniPort() {
        // given
        // when
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS, 1);
        // then
        // expect to be allocated an SNI port
        assertThat(model).isNotNull();
    }

    @Test
    void gatewayConfig() {
        // given
        int sharedSniPort = 1;
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS,
                sharedSniPort);

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(model).fragment();

        // then
        assertThat(gateway).isNotNull();
        assertThat(gateway.name()).isEqualTo(INGRESS_NAME);
        assertThat(gateway.tls()).isNotNull();
        assertThat(gateway.portIdentifiesNode()).isNull();
        assertThat(gateway.sniHostIdentifiesNode()).isNotNull().satisfies(sniStrategy -> {
            assertThat(sniStrategy.bootstrapAddress()).isEqualTo(new HostPort(BOOTSTRAP_ADDRESS, sharedSniPort).toString());
            assertThat(sniStrategy.advertisedBrokerAddressPattern()).isEqualTo(new HostPort(ADVERTISED_BROKER_ADDRESS_PATTERN, 9083).toString());
        });
    }

    @Test
    void doesNotSupplyAnyServices() {
        // given
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS,
                9093);
        assertThat(model).isNotNull();

        // when
        Stream<ServiceBuilder> services = model.services();

        // then
        assertThat(services).isEmpty();
    }

    @Test
    void requestsLoadBalancerServicePorts() {
        // given
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS,
                9093);
        assertThat(model).isNotNull();

        // when
        Stream<Integer> services = model.requiredSniLoadBalancerServicePorts();

        // then
        assertThat(services).containsExactly(9083);
    }

    @Test
    void requestsSharedSniPort() {
        // given
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS,
                9093);

        // when
        boolean requiresSharedSniPort = model.requiresSharedSniContainerPort();

        // then
        assertThat(requiresSharedSniPort).isTrue();
    }

    @Test
    void bootstrapServers() {
        // given
        LoadBalancerClusterIngressNetworkingModel model = new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, TLS,
                9093);

        // when
        String bootstrapServers = model.bootstrapServers();

        // then
        assertThat(bootstrapServers).isEqualTo(new HostPort("my-cluster.kafkaproxy", DEFAULT_CLIENT_FACING_LOADBALANCER_PORT).toString());
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        ThrowableAssert.ThrowingCallable nullIngress = () -> new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, null, LOAD_BALANCER, TLS, 1);
        ThrowableAssert.ThrowingCallable nullCluster = () -> new LoadBalancerClusterIngressNetworkingModel(null, INGRESS, LOAD_BALANCER, TLS, 1);
        ThrowableAssert.ThrowingCallable nullLoadBalancer = () -> new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, null, TLS, 1);
        ThrowableAssert.ThrowingCallable nullTls = () -> new LoadBalancerClusterIngressNetworkingModel(VIRTUAL_KAFKA_CLUSTER, INGRESS, LOAD_BALANCER, null, 1);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("cluster", nullCluster),
                argumentSet("loadBalancer", nullLoadBalancer),
                argumentSet("tls", nullTls));
    }

    @MethodSource
    @ParameterizedTest
    void constructorArgsMustBeNonNull(ThrowableAssert.ThrowingCallable callable) {
        assertThatThrownBy(callable).isInstanceOf(NullPointerException.class);
    }

}
