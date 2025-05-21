/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

import edu.umd.cs.findbugs.annotations.Nullable;

public record LoadBalancerDefinition(KafkaProxyIngress ingress, LoadBalancer loadBalancer, @Nullable Tls tls) implements IngressDefinition {

    public LoadBalancerDefinition {
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(loadBalancer);
    }

    private record LoadBalancerIngressModel(LoadBalancerDefinition definition, int sharedSniPort) implements IngressModel {

        public static final int DEFAULT_LOADBALANCER_PORT = 9083;

        @Override
        public Stream<ServiceBuilder> services() {
            return Stream.empty();
        }

        @Override
        public Stream<ContainerPort> proxyContainerPorts() {
            return Stream.empty();
        }

        @Override
        public NodeIdentificationStrategy nodeIdentificationStrategy() {
            return new SniHostIdentifiesNodeIdentificationStrategy(new HostPort(definition.loadBalancer.getBootstrapAddress(), sharedSniPort).toString(),
                    new HostPort(definition.loadBalancer.getAdvertisedBrokerAddressPattern(), DEFAULT_LOADBALANCER_PORT).toString());
        }

        @Override
        public Optional<Tls> downstreamTls() {
            return Optional.ofNullable(definition.tls());
        }

        @Override
        public boolean requiresSharedSniPort() {
            return definition().requiresSharedSniPort();
        }

        @Override
        public Stream<Integer> requiredSniLoadbalancerPorts() {
            return Stream.of(DEFAULT_LOADBALANCER_PORT);
        }
    }

    @Override
    public IngressModel createIngressModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort, @Nullable Integer sharedSniPort) {
        Objects.requireNonNull(sharedSniPort);
        return new LoadBalancerIngressModel(this, sharedSniPort);
    }

    @Override
    public boolean requiresSharedSniPort() {
        return true;
    }
}
