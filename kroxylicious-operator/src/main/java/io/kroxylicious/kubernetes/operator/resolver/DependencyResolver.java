/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

/**
 * DependencyResolver resolves the dependencies of a KafkaProxy or VirtualKafkaCluster. We use numerous
 * Custom Resources to model the virtual clusters, ingresses, filters and clusters that will eventually
 * manifest as a single proxy Deployment.
 * <p>
 * DependencyResolver is responsible for resolving all of these references into Custom Resources, returning
 * a result that contains the resolved Custom Resource instances, and a description of which references could not
 * be resolved or were resolved but have a status condition declaring that the resource has unresolved
 * references.
 * </p>
 */
public interface DependencyResolver {
    /**
     * Resolves all dependencies of a KafkaProxy recursively (if there are dependencies
     * of dependencies, we resolve them too).
     *
     * @param proxy proxy
     * @param context reconciliation context for a KafkaProxy
     * @return a resolution result containing all resolved resources, and a description of which resources could not be resolved
     */
    ResolutionResult resolveProxyRefs(KafkaProxy proxy, Context<?> context);

    /**
     * Resolves all dependencies of a VirtualKafkaCluster recursively (if there are dependencies
     * of dependencies, we resolve them too).
     *
     * @param cluster cluster being resolved
     * @param context reconciliation context for a VirtualKafkaCluster
     * @return a resolution result containing all resolved resources, and a description of which resources could not be resolved
     */
    ResolutionResult resolveClusterRefs(VirtualKafkaCluster cluster, Context<?> context);

}
