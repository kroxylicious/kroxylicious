/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;

public class KafkaProxyContext {

    private static final String KEY_SEC = "sec";
    private static final String KEY_MODEL = "model";
    private static final String KEY_CLOCK = "clock";

    private KafkaProxyContext() {
    }

    static void init(Clock clock,
                     SecureConfigInterpolator secureConfigInterpolator,
                     ProxyModel model,
                     Context<KafkaProxy> context) {
        var rc = context.managedWorkflowAndDependentResourceContext();

        rc.put(KEY_CLOCK, Clock.fixed(clock.instant(), clock.getZone()));

        rc.put(KEY_SEC, secureConfigInterpolator);

        rc.put(KEY_MODEL, model);
    }

    static Clock clock(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(KEY_CLOCK, Clock.class);
    }

    static SecureConfigInterpolator secureConfigInterpolator(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(KEY_SEC, SecureConfigInterpolator.class);
    }

    static ProxyModel model(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(KEY_MODEL, ProxyModel.class);
    }

    static boolean isBroken(Context<KafkaProxy> context, VirtualKafkaCluster cluster) {
        var model = KafkaProxyContext.model(context);
        var fullyResolved = model.resolutionResult().fullyResolvedClustersInNameOrder().stream().map(ResourcesUtil::name).collect(Collectors.toSet());
        return !fullyResolved.contains(ResourcesUtil.name(cluster))
                || !model.ingressModel().clusterIngressModel(cluster).stream()
                        .allMatch(ingressModel -> ingressModel.ingressExceptions().isEmpty());

    }

}
