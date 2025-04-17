/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * Precondition that's met iff there's a well-formed proxy configuration
 */
public class ProxyConfigReconcilePrecondition implements Condition<ConfigMap, KafkaProxy> {

    @Override
    public boolean isMet(DependentResource<ConfigMap, KafkaProxy> dependentResource,
                         KafkaProxy primary,
                         Context<KafkaProxy> context) {
        return KafkaProxyContext.proxyContext(context).configuration().isPresent();
    }

}
