/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.net;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.config.VirtualCluster;

public interface EndpointReconciler {

    /**
     * Reconciles the existing set endpoints for this virtual cluster against those required for the
     * current set of nodes for this virtual cluster.  Once any necessary alterations to the
     * endpoint bindings have been realised, the returned CompletionStage will be completed.
     *
     * @param virtualCluster virtual cluster
     * @param nodeIds        current set of node ids
     * @return CompletionStage that is used to signal completion of the work.
     */
    CompletionStage<Void> reconcile(VirtualCluster virtualCluster, Set<Integer> nodeIds);
}
