/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaproxy;

import java.util.Set;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class VirtualKafkaClusterToKafkaProxySecondaryToPrimaryMapperTest {

    @Test
    void clusterToProxyMapper() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }

    @Test
    void clusterToProxyMapperIgnoresClusterWithStaleStatus() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> eventSourceContext = MapperTestSupport.mockContextContaining(proxy);
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(eventSourceContext);

        // when
        VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 2L, 1L).build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);

        // then
        assertThat(primaryResourceIDs).isEmpty();
    }

    @Test
    void shouldReturnIdsWhenApiServerUnavailable() {
        // given
        KafkaProxy proxy = MapperTestSupport.buildProxy("proxy");
        EventSourceContext<KafkaProxy> context = mock();
        MapperTestSupport.stubFailingListOperationClient(context);
        MapperTestSupport.stubPrimaryCache(context, proxy);
        SecondaryToPrimaryMapper<VirtualKafkaCluster> mapper = new VirtualKafkaClusterSecondaryToKafkaProxyPrimaryMapper(context);

        // when
        VirtualKafkaCluster cluster = MapperTestSupport.baseVirtualKafkaClusterBuilder(proxy, "cluster", 1L, 1L).build();
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(cluster);

        // then
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(proxy));
    }
}
