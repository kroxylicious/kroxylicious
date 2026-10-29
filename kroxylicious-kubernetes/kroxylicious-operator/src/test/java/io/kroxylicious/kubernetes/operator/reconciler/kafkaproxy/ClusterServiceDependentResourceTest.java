/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterServiceDependentResourceTest {

    @Test
    void serviceNameHashesWhenClusterNameExceedsLimit() {
        // Given
        VirtualKafkaCluster longNamedCluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("a".repeat(100)).endMetadata().build();

        // When
        String name = ClusterServiceDependentResource.serviceName(longNamedCluster);

        // Then
        assertThat(name).hasSizeLessThanOrEqualTo(63);
    }

}
