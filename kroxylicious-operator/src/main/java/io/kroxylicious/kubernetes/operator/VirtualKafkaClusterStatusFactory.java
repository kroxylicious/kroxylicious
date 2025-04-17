/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.virtualkafkaclusterstatus.Ingresses;

public class VirtualKafkaClusterStatusFactory extends StatusFactory<VirtualKafkaCluster> {

    public VirtualKafkaClusterStatusFactory(Clock clock) {
        super(clock);
    }

    VirtualKafkaCluster clusterStatusPatch(VirtualKafkaCluster observedIngress,
                                           ResourceState condition, List<Ingresses> ingresses) {
        // @formatter:off
        return new VirtualKafkaClusterBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedIngress.getStatus()).map(VirtualKafkaClusterStatus::getConditions).orElse(List.of()), condition))
                    .withIngresses(ingresses)
                .endStatus()
                .build();
        // @formatter:on
    }

    @Override
    VirtualKafkaCluster newUnknownConditionStatusPatch(VirtualKafkaCluster observedFilter,
                                                       Condition.Type type,
                                                       Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return clusterStatusPatch(observedFilter, ResourceState.of(unknownCondition), List.of());
    }

    @Override
    VirtualKafkaCluster newFalseConditionStatusPatch(VirtualKafkaCluster observedProxy,
                                                     Condition.Type type,
                                                     String reason,
                                                     String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return clusterStatusPatch(observedProxy, ResourceState.of(falseCondition), List.of());
    }

    @Override
    VirtualKafkaCluster newTrueConditionStatusPatch(VirtualKafkaCluster observedProxy,
                                                    Condition.Type type, String checksum) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return clusterStatusPatch(observedProxy, ResourceState.of(trueCondition), List.of());
    }

    @SuppressWarnings("removal")
    @Override
    VirtualKafkaCluster newTrueConditionStatusPatch(VirtualKafkaCluster observedProxy,
                                                    Condition.Type type) {
        return newTrueConditionStatusPatch(observedProxy, type, "");
    }
}
