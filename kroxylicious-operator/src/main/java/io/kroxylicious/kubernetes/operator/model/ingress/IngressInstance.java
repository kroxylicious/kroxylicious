/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.proxy.config.VirtualClusterGateway;

/**
 * Stateful Ingress model that provides:
 * <ol>
 *     <li>
 *         The Gateway Config, that will be added to the virtual clusters gateways
 *     </li>
 *     <li>
 *         The Services that will be manifested for this ingress
 *     </li>
 *     <li>
 *         The container ports that will be exposed on the Proxy pod (will correspond
 *         to the ports configured in the Gateway Config).
 *     </li>
 * </ol>
 */
public interface IngressInstance {

    /**
     * The GatewayConfig to be added to the Virtual Cluster Config added to the proxy config YAML
     * @return Virtual Cluster Gateway
     */
    VirtualClusterGateway gatewayConfig();

    /**
     * Services to be created for this ingress
     * @return a stream of ServiceBuilders
     */
    Stream<ServiceBuilder> services();

    /**
     * ProxyContainerPorts to be added for this ingress
     * @return a stream of ServiceBuilders
     */
    Stream<ContainerPort> proxyContainerPorts();
}
