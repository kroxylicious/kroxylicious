/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;

class SecretSecondaryJoinedOnIngressTrustAnchorRefToVirtualKafkaClusterPrimaryMapperTest {

    @Test
    void canMapFromSecretTrustAnchorRefToVirtualKafkaClusterWithTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport
                .mockContextContaining(MapperTestSupport.CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR);

        // When
        var mapper = new ResourceSecondaryJoinedOnIngressTrustAnchorRefToVirtualKafkaClusterPrimaryMapper<>(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.TRUST_ANCHOR_PEM_SECRET);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(MapperTestSupport.CLUSTER_TLS_NO_FILTERS_WITH_SECRET_TRUST_ANCHOR));
    }

    @Test
    void canMapFromSecretTrustAnchorRefToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(MapperTestSupport.CLUSTER_NO_FILTERS);

        // When
        var mapper = new ResourceSecondaryJoinedOnIngressTrustAnchorRefToVirtualKafkaClusterPrimaryMapper<>(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.TRUST_ANCHOR_PEM_SECRET);
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void canMapFromSecretTrustAnchorRefToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTrustAnchor() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(MapperTestSupport.CLUSTER_TLS_NO_FILTERS);

        // When
        var mapper = new ResourceSecondaryJoinedOnIngressTrustAnchorRefToVirtualKafkaClusterPrimaryMapper<>(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.TRUST_ANCHOR_PEM_SECRET);
        assertThat(primaryResourceIDs).isEmpty();
    }
}
