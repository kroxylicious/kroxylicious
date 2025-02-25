/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Objects;

import io.fabric8.kubernetes.api.model.apps.Deployment;
import io.fabric8.kubernetes.api.model.apps.DeploymentStatus;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.dependent.DependentResource;
import io.javaoperatorsdk.operator.processing.dependent.workflow.Condition;

import io.kroxylicious.kubernetes.proxy.api.v1alpha1.Proxy;

public class DeploymentReadyCondition implements Condition<Deployment, Proxy> {
    @Override
    public boolean isMet(DependentResource<Deployment, Proxy> dependentResource, Proxy primary, Context<Proxy> context) {
        var optionalResource = dependentResource.getSecondaryResource(primary, context);
        if (optionalResource.isEmpty()) {
            return false;
        }
        var deployment = optionalResource.get();
        DeploymentStatus status = deployment.getStatus();
        return Objects.equals(status.getReadyReplicas(), status.getReplicas());
    }
}
