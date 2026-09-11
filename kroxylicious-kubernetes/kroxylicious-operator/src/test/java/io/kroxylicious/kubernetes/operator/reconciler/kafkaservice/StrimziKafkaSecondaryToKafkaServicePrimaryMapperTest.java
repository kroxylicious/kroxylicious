/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.KAFKA;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static org.assertj.core.api.Assertions.assertThat;

class StrimziKafkaSecondaryToKafkaServicePrimaryMapperTest {

    @Test
    void canMapFromStrimziKafkaToKafkaService() {
        // Given
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(SERVICE);
        var mapper = new StrimziKafkaSecondaryToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(KAFKA);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE));
    }

    @Test
    void canMapFromNamespacedStrimziKafkaToKafkaServiceInDifferentNamespace() {
        // Given
        KafkaService service = new KafkaServiceBuilder(SERVICE)
                .editMetadata()
                .withNamespace("my-proxy")
                .endMetadata()
                .editSpec()
                .editStrimziKafkaRef()
                .withNamespace("my-kafka")
                .endStrimziKafkaRef()
                .endSpec()
                .build();
        var kafka = new KafkaBuilder(KAFKA)
                .editMetadata()
                .withNamespace("my-kafka")
                .endMetadata()
                .build();
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(service);
        var mapper = new StrimziKafkaSecondaryToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(kafka);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(service));
    }

}
