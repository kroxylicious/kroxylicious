/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE_WITH_TLS;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.STRIMZI_PEM_SECRET;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaServicePrimaryToStrimziCaCertificateSecondaryMapperTest {

    @Test
    void canMapFromKafkaServiceToStrimziCaCertificate() {
        // Given
        var mapper = new KafkaServicePrimaryToStrimziCaCertificateSecondaryMapper();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE_WITH_TLS);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(STRIMZI_PEM_SECRET));
    }

    @Test
    void canMapFromKafkaServiceWithoutTrustAnchorToConfigMap() {
        // Given
        var mapper = new KafkaServicePrimaryToResourceSecondaryJoinedOnTlsTrustAnchorRefMapper();
        var serviceNoTrustAnchor = new KafkaServiceBuilder(SERVICE_WITH_TLS).editSpec().withStrimziKafkaRef(null).endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoTrustAnchor);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }
}
