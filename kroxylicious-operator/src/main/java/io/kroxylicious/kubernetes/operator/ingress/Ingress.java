/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.ingress;

import java.util.stream.Stream;

import io.fabric8.kubernetes.api.model.ContainerPort;
import io.fabric8.kubernetes.api.model.ServiceBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.proxy.config.VirtualClusterGateway;

public interface Ingress {

    KafkaProxyIngress resource();

    int proxyPortsRequired();

    VirtualClusterGateway gatewayConfig(int proxyPortRangeStart, int proxyPortRangeEnd);

    Stream<ServiceBuilder> services(int proxyPortRangeStart, int proxyPortRangeEnd);

    Stream<ContainerPort> proxyContainerPorts(int startPortInc, int endPortExc);
}
