/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.model.ingress;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * IngressDefinition definition for a single VirtualKafkaCluster, KafkaProxyIngress pair that:
 * <ol>
 *     <li>declares the requirements of the Ingress (e.g. how many identifying ports it requires)</li>
 *     <li>can instantiate IngressModel</li>
 * </ol>
 * Corresponds to a single ingress item in the VirtualKafkaCluster spec.ingresses
 */
interface IngressDefinition {

    /**
     * The raw resource that was translated into this definition
     * @return resource
     */
    KafkaProxyIngress ingress();

    /**
     * Create an IngressModel with identifying ports allocated to it. Identifying meaning that
     * the port on the container is expected to unambiguously identify which node the client is connecting to.
     * I.e. using a port-per-broker strategy at the proxy.
     *
     * @param firstIdentifyingPort the first identifying port allocated to this Ingress (if definition required identifying ports)
     * @param lastIdentifyingPort the last identifying port (inclusive) allocated to this Ingress  (if definition required identifying ports)
     * @param sharedSniPort the shared SNI port (if definition required SNI port)
     * @return a non-null IngressModel
     */
    IngressModel createIngressModel(@Nullable Integer firstIdentifyingPort, @Nullable Integer lastIdentifyingPort, @Nullable Integer sharedSniPort);

    /**
     * Some Ingress strategies require a set of ports in the proxy pod to be unique and exclusive so that the Proxy
     * can use the client's connection port to identify the cluster and upstream node id they want to communicate with.
     *
     * @return the number of identifying ports this ingress requires
     */
    default int numIdentifyingPortsRequired() {
        return 0;
    }

    default boolean requiresSharedSniPort() {
        return false;
    }

}
