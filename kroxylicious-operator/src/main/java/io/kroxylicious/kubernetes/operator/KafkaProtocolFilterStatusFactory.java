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
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

public class KafkaProtocolFilterStatusFactory extends StatusFactory<KafkaProtocolFilter> {

    public KafkaProtocolFilterStatusFactory(Clock clock) {
        super(clock);
    }

    private KafkaProtocolFilter filterStatusPatch(KafkaProtocolFilter observedProxy,
                                                  Condition condition) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedProxy))
                    .withName(ResourcesUtil.name(observedProxy))
                    .withNamespace(ResourcesUtil.namespace(observedProxy))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedProxy))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedProxy.getStatus()).map(KafkaProtocolFilterStatus::getConditions).orElse(List.of()), new ResourceState(condition)))
                .endStatus()
                .build();
        // @formatter:on
    }

    @Override
    KafkaProtocolFilter newUnknownConditionStatusPatch(KafkaProtocolFilter observedFilter,
                                                       Condition.Type type,
                                                       Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return filterStatusPatch(observedFilter, unknownCondition);
    }

    @Override
    KafkaProtocolFilter newFalseConditionStatusPatch(KafkaProtocolFilter observedProxy,
                                                     Condition.Type type,
                                                     String reason,
                                                     String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return filterStatusPatch(observedProxy, falseCondition);
    }

    @Override
    KafkaProtocolFilter newTrueConditionStatusPatch(KafkaProtocolFilter observedProxy,
                                                    Condition.Type type) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return filterStatusPatch(observedProxy, trueCondition);
    }

}
