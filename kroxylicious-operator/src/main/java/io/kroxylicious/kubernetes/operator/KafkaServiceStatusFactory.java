/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nullable;

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
                                            Condition condition,
                                            String checksum,
                                            @Nullable String bootstrapServers) {
        // @formatter:off
        var metadataBuilder = new KafkaServiceBuilder()
                .withNewMetadata()
                    .withUid(ResourcesUtil.uid(observedIngress))
                    .withName(ResourcesUtil.name(observedIngress))
                    .withNamespace(ResourcesUtil.namespace(observedIngress));
        if (!checksum.equals(MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED)) {
            Annotations.annotateWithReferentChecksum(metadataBuilder, checksum);
        }

        KafkaServiceBuilder service = metadataBuilder
                .endMetadata()
                .withNewStatus()
                    .withObservedGeneration(ResourcesUtil.generation(observedIngress))
                    .withConditions(ResourceState.newConditions(Optional.ofNullable(observedIngress.getStatus()).map(KafkaServiceStatus::getConditions).orElse(List.of()), ResourceState.of(condition)))
                    .withBootstrapServers(bootstrapServers)
                .endStatus();

        return service.build();
        // @formatter:on
    }

    KafkaService newUnknownConditionStatusPatch(KafkaService observedFilter,
                                                Condition.Type type,
                                                Exception e) {
        Condition unknownCondition = newUnknownCondition(observedFilter, type, e);
        return serviceStatusPatch(observedFilter, unknownCondition, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED, null);
    }

    KafkaService newFalseConditionStatusPatch(KafkaService observedProxy,
                                              Condition.Type type,
                                              String reason,
                                              String message) {
        Condition falseCondition = newFalseCondition(observedProxy, type, reason, message);
        return serviceStatusPatch(observedProxy, falseCondition, MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED, null);
    }

    KafkaService newTrueConditionStatusPatch(KafkaService observedProxy,
                                             Condition.Type type,
                                             String checksum) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return serviceStatusPatch(observedProxy, trueCondition, checksum, null);
    }

    KafkaService newTrueConditionStatusPatch(KafkaService observedProxy,
                                             Condition.Type type,
                                             String checksum,
                                             String bootstrapServers) {
        Condition trueCondition = newTrueCondition(observedProxy, type);
        return serviceStatusPatch(observedProxy, trueCondition, checksum, bootstrapServers);
    }

    @SuppressWarnings("removal")
    KafkaService newTrueConditionStatusPatch(KafkaService observedProxy,
                                             Condition.Type type) {
        throw new IllegalStateException("Use newTrueConditionStatusPatch(KafkaService, Condition.Type, String) instead");
    }
}
