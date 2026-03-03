/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.networking;

import java.util.stream.Stream;

import io.kroxylicious.kubernetes.operator.Annotations;

/**
 * Describes the shared LoadBalancer Service requirements of a single VirtualKafkaCluster.spec.ingresses element.
 * Many cluster-ingresses can share a single LoadBalancer Service for SNI, but may place their own requirements on it,
 * such as which ports need to be exposed to clients and which bootstrap servers should be included in the Service's
 * annotations.
 */
public interface SharedLoadBalancerServiceRequirements {

    /**
     * @return the client facing ports that should be exposed on the shared SNI LoadBalancer Service, mapping
     * to the shared SNI port on the proxy.
     */
    Stream<Integer> requiredClientFacingPorts();

    /**
     * @return the bootstrapServers that should be included in the shared SNI LoadBalancer Service metadata
     */
    Annotations.ClusterIngressBootstrapServers bootstrapServersToAnnotate();

}
