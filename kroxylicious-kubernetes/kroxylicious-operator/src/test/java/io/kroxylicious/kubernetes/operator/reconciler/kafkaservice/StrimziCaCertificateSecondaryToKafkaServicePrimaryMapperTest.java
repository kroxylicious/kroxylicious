/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.KubernetesResourceList;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;

import static io.kroxylicious.kubernetes.operator.reconciler.kafkaservice.MapperTestSupport.SERVICE_WITH_TLS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class StrimziCaCertificateSecondaryToKafkaServicePrimaryMapperTest {

    @Test
    void canMapFromStrimziCaCertificateToKafkaService() {
        // Given
        EventSourceContext<KafkaService> eventSourceContext = mock();
        KubernetesClient client = mock();
        when(eventSourceContext.getClient()).thenReturn(client);

        KubernetesResourceList<KafkaService> mockList = MapperTestSupport.mockKafkaServiceListOperation(client);
        when(mockList.getItems()).thenReturn(List.of(SERVICE_WITH_TLS));

        var mapper = new StrimziCaCertificateSecondaryToKafkaServicePrimaryMapper(eventSourceContext);

        // When
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(new SecretBuilder().withNewMetadata().withName("my-cluster-cluster-ca-cert").endMetadata().build());

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(SERVICE_WITH_TLS));
    }
}
