/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.processing.event.ResourceID;

import static org.assertj.core.api.Assertions.assertThat;

class VirtualKafkaClusterPrimarytoConfigMapSecondaryMapperTest {

    @Test
    void canMapFromVirtualKafkaClusterWithTrustAnchorToConfigMap() {
        // Given
        var mapper = new VirtualKafkaClusterPrimaryToConfigMapSecondary();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(MapperTestSupport.CLUSTER_TLS_NO_FILTERS_WITH_TRUST_ANCHOR);

        // Then
        assertThat(secondaryResourceIDs).containsExactly(ResourceID.fromResource(MapperTestSupport.PEM_CONFIG_MAP));
    }

    @Test
    void canMapFromVirtualKafkaClusterWithTlsToConfigMap() {
        // Given
        var mapper = new VirtualKafkaClusterPrimaryToConfigMapSecondary();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(MapperTestSupport.CLUSTER_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromVirtualKafkaClusterWithoutTrustAnchorToConfigMap() {
        // Given
        var mapper = new VirtualKafkaClusterPrimaryToConfigMapSecondary();

        // When
        var secondaryResourceIDs = mapper.toSecondaryResourceIDs(MapperTestSupport.CLUSTER_TLS_NO_FILTERS);

        // Then
        assertThat(secondaryResourceIDs).isEmpty();
    }

}
