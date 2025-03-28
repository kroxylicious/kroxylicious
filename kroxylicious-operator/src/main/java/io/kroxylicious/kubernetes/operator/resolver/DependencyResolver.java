/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * DependencyResolver resolves the dependencies of a KafkaProxy. We use numerous Custom Resources
 * to model the virtual clusters, ingresses, filters and cluster references that will eventually
 * manifest as a single proxy Deployment. The custom resources reference each other by name, and
 * sometimes group/kind.
 * <p>
 * DependencyResolver is responsible for resolving all of these references, returning a result that
 * contains the resolved Custom Resource instances, and a description of which references could not
 * be resolved.
 * </p>
 */
public interface DependencyResolver {
    /**
     * Resolves all dependencies of a KafkaProxy recursively (if there are dependencies
     * of dependencies, we resolve them too).
     *
     * @param context reconciliation context for a KafkaProxy
     * @return a resolution result containing all resolved resources, and a description of which resources could not be resolved
     */
    ResolutionResult deepResolve(Context<KafkaProxy> context);

}
