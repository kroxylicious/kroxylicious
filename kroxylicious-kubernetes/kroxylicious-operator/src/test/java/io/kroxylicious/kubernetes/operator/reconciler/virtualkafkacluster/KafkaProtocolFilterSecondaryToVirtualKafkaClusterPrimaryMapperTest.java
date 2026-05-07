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

class KafkaProtocolFilterSecondaryToVirtualKafkaClusterPrimaryMapperTest {

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
