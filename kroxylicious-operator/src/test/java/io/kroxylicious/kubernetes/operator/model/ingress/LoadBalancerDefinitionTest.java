/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.stream.Stream;

import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancerBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRanges;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicespec.NodeIdRangesBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.TlsBuilder;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;
import io.kroxylicious.proxy.config.VirtualClusterGateway;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class LoadBalancerDefinitionTest {
    private static final String INGRESS_NAME = "my-ingress";
    private static final String CLUSTER_NAME = "my-cluster";
    private static final String PROXY_NAME = "my-proxy";
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
    private static final String PROXY_UID = "my-proxy-uid";
    private static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).withUid(PROXY_UID).withNamespace(NAMESPACE).endMetadata()
            .build();
    private static final NodeIdRanges NODE_ID_RANGE = createNodeIdRange("a", 1L, 3L);

    @Test
    void createInstancesWithExpectedSniPort() {
        // given
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, null);
        // when
        // then
        // expect to be allocated an SNI port
        assertThat(definition.createIngressModel(null, null, 1)).isNotNull();
    }

    @Test
    void shouldRejectNullSniPort() {
        // given
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, null);
        // when
        // then
        // more ports than the 4 required by the node id range + 1 for bootstrap
        assertThatThrownBy(() -> definition.createIngressModel(null, null, null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void gatewayConfig() {
        // given
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, TLS);
        // 3 ports for the nodeId range + 1 for bootstrap
        int sharedSniPort = 9291;
        IngressModel instance = definition.createIngressModel(null, null, sharedSniPort);

        // when
        VirtualClusterGateway gateway = KafkaProxyReconciler.buildVirtualClusterGateway(instance).fragment();

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
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, TLS);
        IngressModel instance = definition.createIngressModel(null, null, 9093);
        assertThat(instance).isNotNull();

        // when
        Stream<ServiceBuilder> services = instance.services();

        // then
        assertThat(services).isEmpty();
    }

    @Test
    void requestsLoadBalancerServicePorts() {
        // given
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, TLS);
        IngressModel instance = definition.createIngressModel(null, null, 9093);
        assertThat(instance).isNotNull();

        // when
        Stream<Integer> services = instance.requiredSniLoadbalancerPorts();

        // then
        assertThat(services).containsExactly(9083);
    }

    @Test
    void requestsSharedSniPort() {
        // given
        LoadBalancerDefinition definition = new LoadBalancerDefinition(INGRESS, LOAD_BALANCER, TLS);
        IngressModel instance = definition.createIngressModel(null, null, 9093);
        assertThat(instance).isNotNull();

        // when
        boolean requiresSharedSniPort = instance.requiresSharedSniPort();

        // then
        assertThat(requiresSharedSniPort).isTrue();
    }

    public static Stream<Arguments> constructorArgsMustBeNonNull() {
        ThrowableAssert.ThrowingCallable nullIngress = () -> new LoadBalancerDefinition(null, LOAD_BALANCER, TLS);
        ThrowableAssert.ThrowingCallable nullLoadBalancer = () -> new LoadBalancerDefinition(INGRESS, null, TLS);
        return Stream.of(argumentSet("ingress", nullIngress),
                argumentSet("loadBalancer", nullLoadBalancer));
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
