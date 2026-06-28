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

import io.kroxylicious.kubernetes.api.common.FilterRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapperTest {

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // Regression for #4017. The mapper previously called client.list() on every secondary event.
        // JOSDK catches exceptions in the informer's event-dispatch path and silently drops the event
        // with no retry — a transient KubernetesClientException therefore left the VKC stuck.
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withFilterRefs(new FilterRefBuilder().withName("filter").build()).endSpec().build();

        EventSourceContext<VirtualKafkaCluster> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, cluster);

        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper(context);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").endMetadata().withNewSpec().endSpec().build();

        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void filterSecondaryToPrimaryMapper() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withFilterRefs(new FilterRefBuilder().withName("filter").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
    }

    @Test
    void filterSecondaryToPrimaryMapperHandlesNullFilterRefs() {
        // given
        VirtualKafkaCluster clusterWithNullFilterRefs = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec().endSpec()
                .build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(clusterWithNullFilterRefs);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        KafkaProtocolFilter filter = new KafkaProtocolFilterBuilder().withNewMetadata().withName("filter").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(filter);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void filterSecondaryToPrimaryMapperIgnoresFilterWithStaleStatus() {
        // given
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withFilterRefs(new FilterRefBuilder().withName("filter").build()).endSpec().build();
        EventSourceContext<VirtualKafkaCluster> eventSourceContext = MapperTestSupport.mockContextContaining(cluster);
        SecondaryToPrimaryMapper<KafkaProtocolFilter> mapper = new KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        // @formatter:off
        KafkaProtocolFilter ingress = new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                .withName("filter")
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
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(ingress);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

}
