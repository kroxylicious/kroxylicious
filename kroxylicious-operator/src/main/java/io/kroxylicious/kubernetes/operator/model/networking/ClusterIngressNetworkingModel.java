/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.Optional;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;

/**
 * Represents the client-facing resources, and backend plumbing required to enable a client to
 * connect to a Virtual Kafka Cluster on one of it's declared ingresses.
 * Client-facing resources could include one-of:
 * <ul>
 *     <li>kubernetes ClusterIP Services for in-cluster access</li>
 *     <li>kubernetes LoadBalancer Services for off-cluster access</li>
 * </ul>
 * Backend plumbing will include:
 * <ul>
 *     <li>container ports exposed on the Proxy Pod</li>
 *     <li>gateway configuration in the Proxy Config/li>
 * </ul>
 */
public interface ClusterIngressNetworkingModel {

    /**
     * The VirtualKafkaCluster underlying this model
     * @return a VirtualKafkaCluster
     */
    VirtualKafkaCluster cluster();

    /**
     * The KafkaProxyIngress underlying this model
     * @return a KafkaProxyIngress
     */
    KafkaProxyIngress ingress();

    /**
     * Kubernetes Services to be created for this model
     * @return a stream of ServiceBuilders
     */
    Stream<ServiceBuilder> services();

    /**
     * ContainerPorts to be added to the proxy for this model. These ports will be used to uniquely
     * identify an upstream node, so they cannot be shared with other ClusterIngressNetworkingModel
     * instances.
     * @return a stream of ContainerPorts
     */
    Stream<ContainerPort> identifyingProxyContainerPorts();

    /**
     * The node identification strategy to be injected into the Proxy Config for this model
     * @return a NodeIdentificationStrategy
     */
    NodeIdentificationStrategy nodeIdentificationStrategy();

    /**
     * The downstream TLS to be injected into the Proxy Config for this model, if available
     */
    Optional<Tls> downstreamTls();

    /**
     * @return true if this cluster ingress requires a shared SNI port in the proxy container to be provided
     */
    default boolean requiresSharedSniContainerPort() {
        return false;
    }

    /**
     * @return the client facing ports that should be exposed on the shared SNI loadbalancer service
     */
    default Stream<Integer> requiredSniLoadBalancerServicePorts() {
        return Stream.empty();
    }

    /**
     * @return the bootstrap servers that clients will connect to
     */
    String bootstrapServers();
}
