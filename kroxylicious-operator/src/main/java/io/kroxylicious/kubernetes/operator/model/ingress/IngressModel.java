/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.Optional;
import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterspec.ingresses.Tls;
import io.kroxylicious.proxy.config.NodeIdentificationStrategy;

/**
 * Kubernetes Ingress model representing a single VirtualCluster Gateway in the Proxy Config
 * and the kubernetes resources required to expose it to clients. It is responsible for
 * providing:
 * <ol>
 *     <li>
 *         The Gateway Config, that will be added to the virtual cluster's gateways
 *     </li>
 *     <li>
 *         The Services that will be manifested for this IngressModel
 *     </li>
 *     <li>
 *         The identifying container ports that will be exposed on the Proxy pod (will correspond
 *         to the ports configured in the Gateway Config).
 *     </li>
 *     <li>
 *         Describing whether this IngressModel requires a shared TLS port to be provisioned
 *     </li>
 * </ol>
 */
public interface IngressModel {

    /**
     * Kubernetes Services to be created for this IngressModel
     * @return a stream of ServiceBuilders
     */
    Stream<ServiceBuilder> services();

    /**
     * ContainerPorts to be added to the proxy for this IngressModel
     * @return a stream of ServiceBuilders
     */
    Stream<ContainerPort> proxyContainerPorts();

    /**
     *
     * @return true iff this IngressModel requires the shared TLS port to be provided
     */
    boolean requiresSharedTLSPort();

    /**
     * The node identification strategy to be injected into the Proxy Config for this IngressModel
     * @return a NodeIdentificationStrategy
     */
    NodeIdentificationStrategy nodeIdentificationStrategy();

    /**
     * The downstream TLS to be injected into the Proxy Config for this IngressModel, if available
     */
    Optional<Tls> downstreamTls();
}
