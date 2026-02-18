/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE;
import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.mockKafkaServiceListOperation;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ConfigMapSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapperTest {

    @Test
    void canMapFromConfigMapTrustAnchorRefToKafkaService() {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(SERVICE));

        var mapper = new ConfigMapSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(eventSourceContext);

        // When
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.TRUST_ANCHOR_PEM_CONFIG_MAP);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE));
    }

    static Stream<Arguments> mappingToConfigMapToleratesKafkaServicesWithoutTls() {
        return Stream.of(
                Arguments.argumentSet("without tls", new KafkaServiceBuilder(SERVICE).editSpec().withTls(null).endSpec().build()),
                Arguments.argumentSet("with tls but without trust anchor",
                        new KafkaServiceBuilder(SERVICE).editSpec().editTls().withTrustAnchorRef(null).endTls().endSpec().build()));
    }

    @ParameterizedTest
    @MethodSource
    void mappingToConfigMapToleratesKafkaServicesWithoutTls(KafkaService service) {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(service));

        // When
        var mapper = new ConfigMapSecondaryJoinedOnTlsTrustAnchorRefToKafkaServicePrimaryMapper(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(new ConfigMapBuilder().withNewMetadata().withName("cm").endMetadata().build());
        assertThat(primaryResourceIDs).isEmpty();
    }
}
