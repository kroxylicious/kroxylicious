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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressFluent;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;

public class KafkaProxyIngressStatusFactory extends StatusFactory<KafkaProxyIngress> {

    public KafkaProxyIngressStatusFactory(Clock clock) {
        super(clock);
    }

    private KafkaProxyIngress ingressStatusPatch(KafkaProxyIngress observedIngress,
                                                 Condition condition,
                                                 String checksum) {

        // @formatter:off
        KafkaProxyIngressFluent<KafkaProxyIngressBuilder>.MetadataNested<KafkaProxyIngressBuilder> metadataBuilder = new KafkaProxyIngressBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress));
        if (!checksum.isBlank()) {
            // In practice this condition means that the existing annotation will be left alone.
            metadataBuilder
                    .addToAnnotations(MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION, checksum);
        }
        return metadataBuilder
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaProxyIngressStatus::getConditions).orElse(List.of()), ResourceState.of(condition)))
                .endStatus()
                .build();
        // @formatter:on
    }

    @Override
    KafkaProxyIngress newUnknownConditionStatusPatch(KafkaProxyIngress observedFilter,
                                                     Condition.Type type,
                                                     Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return ingressStatusPatch(observedFilter, unknownCondition, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }

    @Override
    KafkaProxyIngress newFalseConditionStatusPatch(KafkaProxyIngress observedProxy,
                                                   Condition.Type type,
                                                   String reason,
                                                   String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return ingressStatusPatch(observedProxy, falseCondition, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }

    @Override
    KafkaProxyIngress newTrueConditionStatusPatch(KafkaProxyIngress observedProxy,
                                                  Condition.Type type, String checksum) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return ingressStatusPatch(observedProxy, trueCondition, checksum);
    }

    @SuppressWarnings("removal")
    @Override
    KafkaProxyIngress newTrueConditionStatusPatch(KafkaProxyIngress observedProxy,
                                                  Condition.Type type) {
        throw new IllegalStateException("Use newTrueConditionStatusPatch(KafkaProxyIngress, Condition.Type, String) instead");
    }
}
