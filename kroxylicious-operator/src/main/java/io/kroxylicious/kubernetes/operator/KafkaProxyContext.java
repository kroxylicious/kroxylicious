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
import io.kroxylicious.kubernetes.operator.model.ProxyModelBuilder;

public class KafkaProxyContext {

    static final String SEC = "sec";
    static final String MODEL = "model";
    public static final String CLOCK = "clock";

    static void init(Clock clock,
                     SecureConfigInterpolator secureConfigInterpolator,
                     KafkaProxy proxy,
                     Context<KafkaProxy> context) {
        var rc = context.managedWorkflowAndDependentResourceContext();

        rc.put(CLOCK, Clock.fixed(clock.instant(), clock.getZone()));

        rc.put(SEC, secureConfigInterpolator);

        ProxyModelBuilder proxyModelBuilder = ProxyModelBuilder.contextBuilder();
        ProxyModel model = proxyModelBuilder.build(proxy, context);
        rc.put(MODEL, model);
    }

    static Clock clock(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(CLOCK, Clock.class);
    }

    static SecureConfigInterpolator secureConfigInterpolator(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(SEC, SecureConfigInterpolator.class);
    }

    static ProxyModel model(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(MODEL, ProxyModel.class);
    }

    static boolean isBroken(Context<KafkaProxy> context, VirtualKafkaCluster cluster) {
        var model = KafkaProxyContext.model(context);
        var fullyResolved = model.resolutionResult().fullyResolvedClustersInNameOrder().stream().map(ResourcesUtil::name).collect(Collectors.toSet());
        return !(fullyResolved.contains(ResourcesUtil.name(cluster))
                && model.ingressModel().clusterIngressModel(cluster).stream()
                        .allMatch(ingressModel -> ingressModel.ingressExceptions().isEmpty()));

    }

}
