/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyConfigStateDependentResourceTest {

    @Test
    void configMapNameHashesWhenProxyNameExceedsLimit() {
        // Given
        KafkaProxy longNamedProxy = new KafkaProxyBuilder().withNewMetadata().withName("a".repeat(300)).endMetadata().build();

        // When
        String name = ProxyConfigStateDependentResource.configMapName(longNamedProxy);

        // Then
        assertThat(name).hasSizeLessThanOrEqualTo(253);
    }

}
