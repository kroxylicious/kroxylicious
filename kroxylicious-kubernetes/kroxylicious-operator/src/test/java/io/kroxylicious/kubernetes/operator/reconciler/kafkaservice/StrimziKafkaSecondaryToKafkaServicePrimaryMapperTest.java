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

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.KAFKA;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

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
    void shouldReturnIdsWhenApiServerUnavailable() {
        // Given
        EventSourceContext<KafkaService> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, SERVICE);

        var mapper = new StrimziKafkaSecondaryToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(KAFKA);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE));
    }
}
