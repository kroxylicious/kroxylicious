/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.kubernetes.operator;

import java.util.Map;

import io.fabric8.kubernetes.api.model.Service;
import io.javaoperatorsdk.operator.api.config.informer.InformerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.ControllerConfiguration;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceInitializer;
import io.javaoperatorsdk.operator.api.reconciler.Reconciler;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;
import io.javaoperatorsdk.operator.api.reconciler.dependent.Dependent;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

// formatter=off
@ControllerConfiguration(dependents = {
        @Dependent(
                name = ProxyReconciler.CONFIG_DEP,
                type = ProxyConfigSecret.class),
        @Dependent(
                name = ProxyReconciler.DEPLOYMENT_DEP,
                type = ProxyDeployment.class,
                dependsOn = { ProxyReconciler.CONFIG_DEP } // ,
        // readyPostcondition = DeploymentReadyCondition.class
        ),
        @Dependent(
                name = ProxyReconciler.METRICS_DEP,
                type = MetricsService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP },
                useEventSourceWithName = ProxyReconciler.METRICS_DEP
        ),
        @Dependent(
                name = ProxyReconciler.CLUSTERS_DEP,
                type = ClusterService.class,
                dependsOn = { ProxyReconciler.DEPLOYMENT_DEP },
                useEventSourceWithName = ProxyReconciler.CLUSTERS_DEP
        )
})
// formatter=on
public class ProxyReconciler implements Reconciler<KafkaProxy>, EventSourceInitializer<KafkaProxy> {

    public static final String CONFIG_DEP = "config";
    public static final String DEPLOYMENT_DEP = "deployment";
    public static final String METRICS_DEP = "metrics";
    public static final String CLUSTERS_DEP = "clusters";

    public UpdateControl<KafkaProxy> reconcile(KafkaProxy primary,
                                               Context<KafkaProxy> context) {
        return UpdateControl.patchStatus(
                new KafkaProxyBuilder(primary).editOrNewStatus()
                        .withObservedGeneration(primary.getMetadata().getGeneration())
                        .endStatus()
                        .build());
    }

    @Override
    public Map<String, EventSource> prepareEventSources(
                                                        EventSourceContext<KafkaProxy> context) {
        InformerEventSource<Service, KafkaProxy> ies1 = new InformerEventSource<>(
                InformerConfiguration.from(Service.class, context)
                        .withGenericFilter(new MetricsFilter())
                        .build(),
                context);

        InformerEventSource<Service, KafkaProxy> ies2 = new InformerEventSource<>(
                InformerConfiguration.from(Service.class, context)
                        .withGenericFilter(new ClusterFilter())
                        .build(),
                context);

        return Map.of(METRICS_DEP, ies1,
                CLUSTERS_DEP, ies2);
    }
}
