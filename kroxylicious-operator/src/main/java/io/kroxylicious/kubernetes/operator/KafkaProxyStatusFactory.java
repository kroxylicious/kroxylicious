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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

import edu.umd.cs.findbugs.annotations.Nullable;

public class KafkaProxyStatusFactory extends StatusFactory<KafkaProxy> {

    public KafkaProxyStatusFactory(Clock clock) {
        super(clock);
    }

    private KafkaProxy kafkaProxyStatusPatch(KafkaProxy observedProxy,
                                             Condition condition,
                                             @Nullable Integer replicaCount) {
        // @formatter:off
        KafkaProxyBuilder kafkaProxyBuilder = new KafkaProxyBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedProxy))
                    .withName(ResourcesUtil.name(observedProxy))
                    .withNamespace(ResourcesUtil.namespace(observedProxy))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedProxy))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedProxy.getStatus()).map(KafkaProxyStatus::getConditions).orElse(List.of()), ResourceState.of(condition)))
                .endStatus();
        // @formatter:on
        if (replicaCount != null) {
            kafkaProxyBuilder.editStatus().withReplicas(replicaCount).endStatus();
        }
        return kafkaProxyBuilder.build();
    }

    @Override
    KafkaProxy newUnknownConditionStatusPatch(KafkaProxy observedProxy,
                                              Condition.Type type,
                                              Exception e) {
        Condition unknownCondition = newUnknownCondition(observedProxy, type, e);
        return kafkaProxyStatusPatch(observedProxy, unknownCondition, null);
    }

    @Override
    KafkaProxy newFalseConditionStatusPatch(KafkaProxy observedProxy,
                                            Condition.Type type,
                                            String reason,
                                            String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return kafkaProxyStatusPatch(observedProxy, falseCondition, null);
    }

    @Override
    KafkaProxy newTrueConditionStatusPatch(KafkaProxy observedProxy,
                                           Condition.Type type,
                                           String checksum) {
        return newTrueConditionStatusPatch(observedProxy, type, (Integer) null);
    }

    KafkaProxy newTrueConditionStatusPatch(KafkaProxy observedProxy,
                                           Condition.Type type,
                                           @Nullable Integer replicaCount) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return kafkaProxyStatusPatch(observedProxy, trueCondition, replicaCount);
    }

    @SuppressWarnings("removal")
    @Override
    KafkaProxy newTrueConditionStatusPatch(KafkaProxy observedProxy,
                                           Condition.Type type) {
        return newTrueConditionStatusPatch(observedProxy, type, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }
}
