/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxyingress;

import io.fabric8.openshift.api.model.Route;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * Activation condition that's met if the Kubernetes server supports OpenShift Routes
 */
public class IsOpenshiftRouteSupportedActivationCondition
        implements Condition<Route, KafkaProxy> {
    @Override
    public boolean isMet(
                         DependentResource<Route, KafkaProxy> dependentResource,
                         KafkaProxy primary,
                         Context<KafkaProxy> context) {
        return context.getClient().supports(Route.class);
    }
}
