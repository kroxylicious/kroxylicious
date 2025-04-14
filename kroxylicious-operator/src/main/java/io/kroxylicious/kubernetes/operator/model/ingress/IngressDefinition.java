/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;

/**
 * Stateless Ingress model that:
 * <ol>
 *     <li>describes the requirements of the ingress (e.g. how many exclusive ports it requires)</li>
 *     <li>can instantiate IngressInstances, allowing multiple virtual clusters to use the same IngressDefinition</li>
 * </ol>
 */
public interface IngressDefinition {

    /**
     * The raw resource that was translated into this definition
     * @return resource
     */
    KafkaProxyIngress resource();

    /**
     * Create an instance of the Ingress with identifying ports allocated to it. Identifying meaning that
     * the port on the container is expected to unambigiously identify which node the client is connecting to.
     * I.e. using a port-per-broker strategy at the proxy.
     *
     * @param firstIdentifyingPort the first identifying port allocated to this ingress instance
     * @param lastIdentifyingPort the last identifying port (inclusive) allocated to this ingress instance
     * @return a non-null ingress instance
     */
    IngressInstance createInstance(int firstIdentifyingPort, int lastIdentifyingPort);

    /**
     * Some Ingress strategies require a set of ports in the proxy pod to be unique and exclusive so that the Proxy
     * can use the client's connection port to identify the cluster and upstream node id they want to communicate with.
     *
     * @return the number of identifying ports this ingress requires
     */
    int numIdentifyingPortsRequired();

}
