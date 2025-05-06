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
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator;

public class KafkaServiceStatusFactory extends StatusFactory<KafkaService> {

    public KafkaServiceStatusFactory(Clock clock) {
        super(clock);
    }

    private KafkaService serviceStatusPatch(KafkaService observedIngress,
                                            Condition condition) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress))
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of()), ResourceState.of(condition)))
                .endStatus()
                .build();
        // @formatter:on
    }

    KafkaService newUnknownConditionStatusPatch(KafkaService observedFilter,
                                                Condition.Type type,
                                                Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return serviceStatusPatch(observedFilter, unknownCondition);
    }

    KafkaService newFalseConditionStatusPatch(KafkaService observedProxy,
                                              Condition.Type type,
                                              String reason,
                                              String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return serviceStatusPatch(observedProxy, falseCondition);
    }

    KafkaService newTrueConditionStatusPatch(KafkaService observedProxy,
                                             Condition.Type type, String checksum) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return serviceStatusPatch(observedProxy, trueCondition);
    }

    @SuppressWarnings("removal")
    KafkaService newTrueConditionStatusPatch(KafkaService observedProxy,
                                             Condition.Type type) {
        return newTrueConditionStatusPatch(observedProxy, type, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED);
    }
}
