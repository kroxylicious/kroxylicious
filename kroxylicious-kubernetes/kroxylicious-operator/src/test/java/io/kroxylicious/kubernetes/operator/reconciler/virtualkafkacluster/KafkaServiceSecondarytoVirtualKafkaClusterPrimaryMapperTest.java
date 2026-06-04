/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.virtualkafkacluster;

import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.IndexerResourceCache;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;

import io.kroxylicious.kubernetes.api.common.KafkaServiceRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
    void resolvesReferrersFromPrimaryCacheWithoutQueryingApiServer() {
        // given a primary cache containing a referring cluster
        VirtualKafkaCluster cluster = new VirtualKafkaClusterBuilder().withNewMetadata().withName("cluster").endMetadata().withNewSpec()
                .withTargetKafkaServiceRef(new KafkaServiceRefBuilder().withName("target-kafka").build()).endSpec().build();

        EventSourceContext<VirtualKafkaCluster> eventSourceContext = mock();
        IndexerResourceCache<VirtualKafkaCluster> primaryCache = mock();
        when(eventSourceContext.getPrimaryCache()).thenReturn(primaryCache);
        when(primaryCache.list(any(), any())).thenAnswer(invocation -> {
            Predicate<VirtualKafkaCluster> predicate = invocation.getArgument(1);
            return Stream.of(cluster).filter(predicate);
        });

        SecondaryToPrimaryMapper<KafkaService> mapper = new KafkaServiceSecondaryToVirtualKafkaClusterPrimaryMapper(eventSourceContext);
        KafkaService service = new KafkaServiceBuilder().withNewMetadata().withName("target-kafka").endMetadata().withNewSpec().endSpec().build();

        // when
        Set<ResourceID> primaryResourceIDs = mapper.toPrimaryResourceIDs(service);

        // then the referrer is resolved from the cache, and no live list is issued against the API server.
        // The live list (context.getClient()...) was the source of the transient failures that dropped events in #4017.
        assertThat(primaryResourceIDs).containsExactly(ResourceID.fromResource(cluster));
        verify(eventSourceContext, never()).getClient();
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
