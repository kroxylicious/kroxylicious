/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.Set;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE_SECRET_TRUST_ANCHOR;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.TRUST_ANCHOR_PEM_SECRET;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class SecretSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapperTest {

    @Test
    void canMapFromSecretTrustAnchorRefToKafkaService() {
        // Given
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(SERVICE_SECRET_TRUST_ANCHOR);
        var mapper = new SecretSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(TRUST_ANCHOR_PEM_SECRET);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE_SECRET_TRUST_ANCHOR));
    }

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // Given
        EventSourceContext<KafkaService> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, SERVICE_SECRET_TRUST_ANCHOR);

        var mapper = new SecretSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(TRUST_ANCHOR_PEM_SECRET);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE_SECRET_TRUST_ANCHOR));
    }

    static Stream<Arguments> mappingToSecretToleratesKafkaServicesWithoutTls() {
        return Stream.of(
                Arguments.argumentSet("without tls", new KafkaServiceBuilder(SERVICE).editSpec().withTls(null).endSpec().build()),
                Arguments.argumentSet("with tls but without trust anchor",
                        new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build()));
    }

    @ParameterizedTest
    @MethodSource
    void mappingToSecretToleratesKafkaServicesWithoutTls(KafkaService service) {
        // Given
        EventSourceContext<KafkaService> context = MapperTestSupport.mockContextContaining(service);
        var mapper = new SecretSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(context);

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(new SecretBuilder().withNewMetadata().withName("secret").endMetadata().build());

        // Then
        assertThat(primaryResourceIDs).isEmpty();
    }
}
