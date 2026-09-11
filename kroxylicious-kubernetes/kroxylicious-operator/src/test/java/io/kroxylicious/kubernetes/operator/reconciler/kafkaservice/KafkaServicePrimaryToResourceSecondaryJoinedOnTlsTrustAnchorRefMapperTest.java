/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.TRUST_ANCHOR_PEM_CONFIG_MAP;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapperTest {

    @Test
    void canMapFromKafkaServiceWithTrustAnchorToConfigMap() {
        // Given
        var mapper = new KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapper();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(TRUST_ANCHOR_PEM_CONFIG_MAP));
    }

    @Test
    void canMapFromKafkaServiceWithoutTrustAnchorToConfigMap() {
        // Given
        var mapper = new KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapper();
        var serviceNoTrustAnchor = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoTrustAnchor);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }
}
