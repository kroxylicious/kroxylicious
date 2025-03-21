/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.resolver;

import java.util.Comparator;
import java.util.Set;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.LocalRef;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.ClusterCondition;
import io.kroxylicious.kubernetes.operator.KafkaProxyReconciler;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.name;

public record ClusterConditionUnresolvedDependencyReporter(Context<KafkaProxy> context) implements UnresolvedDependencyReporter {

    @Override
    public void reportUnresolvedDependencies(VirtualKafkaCluster cluster, Set<LocalRef<?>> unresolvedDependencies) {
        if (unresolvedDependencies.isEmpty()) {
            throw new IllegalStateException("reporter should not be invoked if there are no unresolved dependencies");
        }
        Comparator<LocalRef<?>> comparator = Comparator.<LocalRef<?>, String> comparing(LocalRef::getGroup)
                .thenComparing(LocalRef::getKind)
                .thenComparing(LocalRef::getName);
        LocalRef<?> firstUnresolvedDependency = unresolvedDependencies.stream()
                .sorted(comparator).findFirst()
                .orElseThrow();
        KafkaProxyReconciler.addClusterCondition(context, cluster,
                ClusterCondition.refNotFound(name(cluster), firstUnresolvedDependency));
    }
}
