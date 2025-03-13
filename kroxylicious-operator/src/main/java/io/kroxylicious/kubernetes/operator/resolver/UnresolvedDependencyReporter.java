/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Set;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

/**
 * Callback that reports unresolved dependencies for a virtual cluster
 */
public interface UnresolvedDependencyReporter {
    /**
     * Invoked when a cluster has one or more unresolved dependencies
     * @param cluster cluster
     * @param unresolvedDependencies non-empty list of unresolved dependencies
     */
    void reportUnresolvedDependencies(VirtualKafkaCluster cluster, Set<ResolutionResult.UnresolvedDependency> unresolvedDependencies);
}
