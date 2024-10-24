/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.operator;

import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;

import io.kroxylicious.crapi.v1alpha1.KafkaProxy;
import io.kroxylicious.crapi.v1alpha1.KafkaProxyBuilder;

@ControllerConfiguration(dependents = {
        @Dependent(type = ProxyConfigSecret.class),
        @Dependent(type = ProxyDeployment.class),
        @Dependent(type = ProxyService.class)
})
public class ProxyReconciler implements Reconciler<KafkaProxy> {

    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        return UpdateControl.patchStatus(
                new KafkaProxyBuilder(primary).editOrNewStatus()
                        .withObservedGeneration(primary.getMetadata().getGeneration())
                        .endStatus()
                        .build());
    }
}
