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
import static org.mockito.Mockito.mock;

class SecretSecondaryJoinedOnIngressCertificateRefToVirtualKafkaClusterPrimaryMapperTest {
    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // Regression for #4017. The mapper previously called client.list() on every secondary event.
        // JOSDK catches exceptions in the informer's event-dispatch path and silently drops the event
        // with no retry — a transient KubernetesClientException therefore left the VKC stuck.
        EventSourceContext<VirtualKafkaCluster> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, MapperTestSupport.CLUSTER_TLS_NO_FILTERS);

        var mapper = new SecretSecondaryJoinedOnIngressCertificateRefToVirtualKafkaClusterPrimaryMapper(context);
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.KUBE_TLS_CERT_SECRET);

        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(MapperTestSupport.CLUSTER_TLS_NO_FILTERS));
    }

    @Test
    void canMapFromSecretToVirtualKafkaClusterWithTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(MapperTestSupport.CLUSTER_TLS_NO_FILTERS);

        // When
        var mapper = new SecretSecondaryJoinedOnIngressCertificateRefToVirtualKafkaClusterPrimaryMapper(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(MapperTestSupport.CLUSTER_TLS_NO_FILTERS));
    }

    @Test
    void canMapFromSecretToVirtualKafkaClusterToleratesVirtualKafkaClusterWithoutTls() {
        // Given
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(MapperTestSupport.CLUSTER_NO_FILTERS);

        // When
        var mapper = new SecretSecondaryJoinedOnIngressCertificateRefToVirtualKafkaClusterPrimaryMapper(eventSourceContext);

        // Then
        var primaryResourceIDs = mapper.toPrimaryResourceIDs(MapperTestSupport.KUBE_TLS_CERT_SECRET);
        assertThat(primaryResourceIDs).isEmpty();
    }

}
