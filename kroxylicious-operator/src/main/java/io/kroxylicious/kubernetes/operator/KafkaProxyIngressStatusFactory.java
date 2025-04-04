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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;

public class KafkaProxyIngressStatusFactory extends StatusFactory<KafkaProxyIngress> {
    public KafkaProxyIngressStatusFactory(Clock clock) {
        super(clock);
    }

    private KafkaProxyIngress ingressStatusPatch(KafkaProxyIngress observedIngress,
                                                 Condition condition) {
        // @formatter:off
        return new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of()), new ResourceState(condition)))
                .endStatus()
                .build();
        // @formatter:on
    }

    @Override
    KafkaProxyIngress newUnknownConditionStatusPatch(KafkaProxyIngress observedFilter,
                                                     Condition.Type type,
                                                     Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return ingressStatusPatch(observedFilter, unknownCondition);
    }

    @Override
    KafkaProxyIngress newFalseConditionStatusPatch(KafkaProxyIngress observedProxy,
                                                   Condition.Type type,
                                                   String reason,
                                                   String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return ingressStatusPatch(observedProxy, falseCondition);
    }

    @Override
    KafkaProxyIngress newTrueConditionStatusPatch(KafkaProxyIngress observedProxy,
                                                  Condition.Type type) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return ingressStatusPatch(observedProxy, trueCondition);
    }
}
