/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.Comparator;
import java.util.Set;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.resolver.ResolutionResult;
import io.kroxylicious.kubernetes.operator.resolver.UnresolvedDependencyReporter;

import static io.kroxylicious.kubernetes.operator.ClusterCondition.ingressNotFound;
import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

record ContextDependencyReporter(Context<KafkaProxy> context) implements UnresolvedDependencyReporter {

    @Override
    public void reportUnresolvedDependencies(VirtualKafkaCluster cluster, Set<ResolutionResult.UnresolvedDependency> unresolvedDependencies) {
        ResolutionResult.UnresolvedDependency firstUnresolvedDependency = unresolvedDependencies.stream()
                .sorted(Comparator.comparing(ResolutionResult.UnresolvedDependency::type).thenComparing(ResolutionResult.UnresolvedDependency::name)).findFirst()
                .orElseThrow();
        switch (firstUnresolvedDependency.type()) {
            case KAFKA_PROXY_INGRESS -> SharedKafkaProxyContext.addClusterCondition(context, cluster, ingressNotFound(name(cluster), firstUnresolvedDependency.name()));
            case FILTER -> SharedKafkaProxyContext.addClusterCondition(context, cluster,
                    ClusterCondition.filterNotFound(name(cluster), firstUnresolvedDependency.name()));
            case KAFKA_CLUSTER_REF -> SharedKafkaProxyContext.addClusterCondition(context, cluster,
                    ClusterCondition.targetClusterRefNotFound(name(cluster), cluster.getSpec().getTargetCluster()));
        }
    }
}
