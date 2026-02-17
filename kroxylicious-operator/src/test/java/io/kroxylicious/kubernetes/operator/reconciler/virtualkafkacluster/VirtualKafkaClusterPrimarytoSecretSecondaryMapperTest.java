/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualKafkaClusterPrimarytoSecretSecondaryMapperTest {
    @Test
    void canMapFromVirtualKafkaClusterWithServerCertToSecret() {
        // Given
        var mapper = new VirtualKafkaClusterPrimaryToSecretSecondary();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(MapperTestSupport.CLUSTER_TLS_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(MapperTestSupport.KUBE_TLS_CERT_SECRET));
    }

    @Test
    void canMapFromVirtualKafkaClusterWithoutServerCertToSecret() {
        // Given
        var mapper = new VirtualKafkaClusterPrimaryToSecretSecondary();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(MapperTestSupport.CLUSTER_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }
}
