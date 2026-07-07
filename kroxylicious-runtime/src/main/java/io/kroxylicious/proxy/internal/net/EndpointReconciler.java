/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.service.HostPort;

public interface EndpointReconciler {

    /**
     * Reconciles the existing set endpoints for this virtual cluster against those required for the
     * current set of nodes for this virtual cluster.  Once any necessary alterations to the
     * endpoint bindings have been realised, the returned CompletionStage will be completed.
     *
     * @param virtualClusterModel virtual cluster
     * @param upstreamNodes  current set of node ids
     * @return CompletionStage that is used to signal completion of the work.
     */
    CompletionStage<Void> reconcile(EndpointGateway virtualClusterModel, Map<Integer, HostPort> upstreamNodes);

    /**
     * Returns the upstream address for the given virtual node ID, as last reconciled for the
     * virtual cluster identified by {@code gateway}. Returns empty if the address is not yet known.
     *
     * @param gateway the virtual cluster's endpoint gateway
     * @param nodeId the virtual node ID
     * @return the upstream host and port, or empty if not yet known
     */
    Optional<HostPort> upstreamAddress(EndpointGateway gateway, int nodeId);
}
