/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.strimzi.api.kafka.model.kafka.Kafka;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceSpec;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;

class StrimziKafkaSecondaryToKafkaServicePrimaryMapper implements SecondaryToPrimaryMapper<Kafka> {
    private static final String STRIMZI_KAFKA_REF_INDEX = "strimziKafkaRef";

    private final EventSourceContext<KafkaService> context;

    StrimziKafkaSecondaryToKafkaServicePrimaryMapper(EventSourceContext<KafkaService> context) {
        this.context = context;
        context.getPrimaryCache().addIndexer(STRIMZI_KAFKA_REF_INDEX, service -> Optional.ofNullable(service.getSpec())
                .map(KafkaServiceSpec::getStrimziKafkaRef)
                .map(strimziKafkaRef -> ResourcesUtil.namespacedName(ResourcesUtil.strimziKafkaNamespace(service, strimziKafkaRef), strimziKafkaRef.getRef().getName()))
                .stream()
                .toList());
    }

    @Override
    public Set<ResourceID> toPrimaryResourceIDs(Kafka kafka) {
        return context.getPrimaryCache()
                .byIndex(STRIMZI_KAFKA_REF_INDEX, ResourcesUtil.namespacedName(ResourcesUtil.namespace(kafka), ResourcesUtil.name(kafka)))
                .stream()
                .map(ResourceID::fromResource)
                .collect(Collectors.toSet());
    }
}
