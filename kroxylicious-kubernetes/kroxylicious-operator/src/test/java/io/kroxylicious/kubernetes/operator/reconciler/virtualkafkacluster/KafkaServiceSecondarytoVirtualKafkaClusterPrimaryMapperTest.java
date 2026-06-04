/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaServiceSecondarytoVirtualKafkaClusterPrimaryMapperTest {

    @Test
    void kafkaServiceSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();

        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        KafkaService service = new KafkaServiceBuilder().withNewMetadata().withName("target-kafka").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // Regression test for #4017. The mapper previously called client.list() on every secondary
        // event. JOSDK catches exceptions thrown from within the informer's event-dispatch path and
        // silently drops the triggering event with no retry — a transient KubernetesClientException
        // on the list therefore left the VirtualKafkaCluster stuck with no ingress status.

        // Given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();

        EventSourceContext<VirtualKafkaCluster> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, cluster);

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToVirtualKafkaClusterPrimaryMapper(context);
        KafkaService service = new KafkaServiceBuilder().withNewMetadata().withName("target-kafka").endMetadata().withNewSpec().endSpec().build();

        // When
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // Then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void kafkaServiceSecondaryToPrimaryMapperIgnoresServiceWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaService service = new KafkaServiceBuilder()
                .withNewMetadata()
                .withName("target-kafka")
                .withGeneration(23L)
                .endMetadata()
                .withNewSpec()
                .endSpec()
                .withNewStatus()
                .withObservedGeneration(20L)
                .endStatus()
                .build();
        // @formatter:on

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }
}
