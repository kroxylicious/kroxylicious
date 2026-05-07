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
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.TLS_SECRET;
import static org.assertj.core.api.Assertions.assertThat;

class KafkaServicePrimaryToSecretSecondaryJoinedOnTlsCertificateRefMapperTest {

    @Test
    void canMapFromKafkaServiceWithClientCertToSecret() {
        // Given
        var mapper = new KafkaServicePrimaryToSecretSecondaryJoinedOnTlsCertificateRefMapper();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(SERVICE);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(TLS_SECRET));
    }

    @Test
    void canMapFromKafkaServiceWithoutClientCertToSecret() {
        // Given
        var mapper = new KafkaServicePrimaryToSecretSecondaryJoinedOnTlsCertificateRefMapper();
        var serviceNoCert = new KafkaServiceBuilder(SERVICE).editSpec().editTls().withCertificateRef(null).endTls().endSpec().build();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(serviceNoCert);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

}
