/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyingressspec.LoadBalancer;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;
import io.kroxylicious.proxy.config.SniHostIdentifiesNodeIdentificationStrategy;
import io.kroxylicious.proxy.service.HostPort;

public record LoadBalancerClusterIngressNetworkingModel(VirtualKafkaCluster cluster,
                                                        KafkaProxyIngress ingress,
                                                        LoadBalancer loadBalancer,
                                                        Tls tls,
                                                        int sharedSniPort)
        implements ClusterIngressNetworkingModel {

    public LoadBalancerClusterIngressNetworkingModel {
        Objects.requireNonNull(cluster);
        Objects.requireNonNull(ingress);
        Objects.requireNonNull(loadBalancer);
        Objects.requireNonNull(tls);
    }

    public static final int DEFAULT_LOADBALANCER_PORT = 9083;

    @Override
    public Stream<ServiceBuilder> services() {
        return Stream.empty();
    }

    @Override
    public Stream<ContainerPort> identifyingProxyContainerPorts() {
        return Stream.empty();
    }

    @Override
    public NodeIdentificationStrategy nodeIdentificationStrategy() {
        return new SniHostIdentifiesNodeIdentificationStrategy(new HostPort(loadBalancer.getBootstrapAddress(), sharedSniPort).toString(),
                new HostPort(loadBalancer.getAdvertisedBrokerAddressPattern(), DEFAULT_LOADBALANCER_PORT).toString());
    }

    @Override
    public Optional<Tls> downstreamTls() {
        return Optional.of(tls);
    }

    @Override
    public boolean requiresSharedSniContainerPort() {
        return true;
    }

    @Override
    public Stream<Integer> requiredSniLoadBalancerServicePorts() {
        return Stream.of(DEFAULT_LOADBALANCER_PORT);
    }
}
