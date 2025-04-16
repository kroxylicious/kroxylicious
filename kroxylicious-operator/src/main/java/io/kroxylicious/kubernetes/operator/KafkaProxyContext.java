/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Optional;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.model.ProxyModel;
import io.kroxylicious.proxy.config.Configuration;

import edu.umd.cs.findbugs.annotations.Nullable;

public record KafkaProxyContext(VirtualKafkaClusterStatusFactory virtualKafkaClusterStatusFactory,
                                ProxyModel model,
                                Optional<ConfigurationFragment<Configuration>> configuration) {

    private static final String KEY_CTX = KafkaProxyContext.class.getName();

    static void init(Context<KafkaProxy> context,
                     VirtualKafkaClusterStatusFactory virtualKafkaClusterStatusFactory,
                     ProxyModel model,
                     @Nullable ConfigurationFragment<Configuration> configuration) {
        var rc = context.managedWorkflowAndDependentResourceContext();

        rc.put(KEY_CTX,
                new KafkaProxyContext(
                        virtualKafkaClusterStatusFactory,
                        model,
                        Optional.ofNullable(configuration)));
    }

    static KafkaProxyContext proxyContext(Context<KafkaProxy> context) {
        return context.managedWorkflowAndDependentResourceContext().getMandatory(KEY_CTX, KafkaProxyContext.class);
    }

    boolean isBroken(VirtualKafkaCluster cluster) {
        var fullyResolved = model().resolutionResult().fullyResolvedClustersInNameOrder().stream().map(ResourcesUtil::name).collect(Collectors.toSet());
        return !fullyResolved.contains(ResourcesUtil.name(cluster))
                || !model().ingressModel().clusterIngressModel(cluster).stream()
                        .allMatch(ingressModel -> ingressModel.ingressExceptions().isEmpty());

    }

}
