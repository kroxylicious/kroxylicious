/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE_WITH_TLS;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.STRIMZI_PEM_SECRET;
import static org.assertj.core.api.Assertions.assertThat;

class StrimziCaCertificateSecondaryToKafkaServicePrimaryMapperTest {

    @Test
    void canMapFromStrimziCaCertificateToKafkaService() {
        // Given
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(SERVICE_WITH_TLS);
        var mapper = new StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(STRIMZI_PEM_SECRET);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE_WITH_TLS));
    }

    @Test
    void canMapFromNamespacedStrimziCaCertificateToKafkaServiceInDifferentNamespace() {
        // Given
        KafkaService service = new KafkaServiceBuilder(SERVICE_WITH_TLS)
                .editMetadata()
                .withNamespace("my-proxy")
                .endMetadata()
                .editSpec()
                .editStrimziKafkaRef()
                .withNamespace("my-kafka")
                .endStrimziKafkaRef()
                .endSpec()
                .build();
        var secret = new SecretBuilder(STRIMZI_PEM_SECRET)
                .editMetadata()
                .withNamespace("my-kafka")
                .endMetadata()
                .build();
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(service);
        var mapper = new StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(secret);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(service));
    }

}
