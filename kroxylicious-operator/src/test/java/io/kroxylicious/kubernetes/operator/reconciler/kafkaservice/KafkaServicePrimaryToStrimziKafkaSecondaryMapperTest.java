/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.KAFKA;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaServicePrimaryToStrimziKafkaSecondaryMapperTest {

    @Test
    void canMapFromKafkaServiceWithStrimziKafkaRefToStrimziKafka() {
        // Given
        var mapper = new KafkaServicePrimaryToStrimziKafkaSecondaryMapper();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(KAFKA));
    }

    @Test
    void canMapFromKafkaServiceWithoutStrimziKafkaRefToStrimziKafka() {
        // Given
        var mapper = new KafkaServicePrimaryToStrimziKafkaSecondaryMapper();
        var serviceNoStrimziKafkaRef = new KafkaServiceBuilder(SERVICE).editSpec().withStrimziKafkaRef(null).endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoStrimziKafkaRef);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }
}
