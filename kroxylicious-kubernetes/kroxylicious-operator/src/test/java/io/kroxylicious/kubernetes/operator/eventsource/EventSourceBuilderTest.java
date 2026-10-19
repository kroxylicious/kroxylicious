/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.eventsource;

import java.util.Optional;
import java.util.Set;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.client.informers.SharedIndexInformer;
import io.javaoperatorsdk.operator.api.reconciler.EventSourceContext;
import io.javaoperatorsdk.operator.processing.event.EventHandler;
import io.javaoperatorsdk.operator.processing.event.ResourceID;
import io.javaoperatorsdk.operator.processing.event.source.EventSource;
import io.javaoperatorsdk.operator.processing.event.source.PrimaryToSecondaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.SecondaryToPrimaryMapper;
import io.javaoperatorsdk.operator.processing.event.source.informer.InformerEventSource;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerEventSource;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises {@link EventSourceBuilder} directly: validation, the choice between a dedicated and a shared
 * informer, and the {@link EventSourceBuilder#requireFreshStatus()} decorator. {@link Ref}'s declarative
 * factories are covered separately by {@link RefTest}.
 */
class EventSourceBuilderTest {

    private static final PrimaryToSecondaryMapper<VirtualKafkaCluster> NO_SECONDARIES = primary -> Set.of();
    private static final SecondaryToPrimaryMapper<KafkaService> NO_PRIMARIES = secondary -> Set.of();

    @Test
    void buildRequiresName() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        var builder = EventSourceBuilder.forSecondary(context, KafkaService.class).withMappers(NO_SECONDARIES, NO_PRIMARIES);

        // When / Then
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void buildRequiresRelationshipOrMappers() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        var builder = EventSourceBuilder.forSecondary(context, KafkaService.class).withName("services");

        // When / Then
        assertThatThrownBy(builder::build).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void withMappersAfterWithRelationshipThrows() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        var builder = EventSourceBuilder.forSecondary(context, KafkaService.class)
                .withName("services")
                .withRelationship(Ref.toSecondary(cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef())));

        // When / Then
        assertThatThrownBy(() -> builder.withMappers(NO_SECONDARIES, NO_PRIMARIES)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void withRelationshipAfterWithMappersThrows() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        var builder = EventSourceBuilder.forSecondary(context, KafkaService.class)
                .withName("services")
                .withMappers(NO_SECONDARIES, NO_PRIMARIES);
        Relationship<VirtualKafkaCluster, KafkaService> secondary = Ref.toSecondary(cluster -> Optional.of(cluster.getSpec().getTargetKafkaServiceRef()));

        // When / Then
        assertThatThrownBy(() -> builder.withRelationship(secondary))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void withoutSharedInformerBuildsDedicatedInformerEventSource() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        when(context.getPrimaryResourceClass()).thenReturn(VirtualKafkaCluster.class);

        // When
        EventSource<KafkaService, VirtualKafkaCluster> eventSource = EventSourceBuilder.forSecondary(context, KafkaService.class)
                .withName("services")
                .withMappers(NO_SECONDARIES, NO_PRIMARIES)
                .build();

        // Then
        assertThat(eventSource).isInstanceOf(InformerEventSource.class);
    }

    @Test
    void withSharedInformerBuildsSharedInformerEventSource() {
        // Given
        EventSourceContext<VirtualKafkaCluster> context = mock();
        SharedInformerManager sharedInformerManager = mock();
        SharedIndexInformer<KafkaService> informer = mock();
        when(sharedInformerManager.getOrCreateInformer(KafkaService.class)).thenReturn(informer);
        when(sharedInformerManager.effectiveNamespaces()).thenReturn(Set.of());

        // When
        EventSource<KafkaService, VirtualKafkaCluster> eventSource = EventSourceBuilder.forSecondary(context, KafkaService.class)
                .withName("services")
                .withSharedInformer(sharedInformerManager)
                .withMappers(NO_SECONDARIES, NO_PRIMARIES)
                .build();

        // Then
        assertThat(eventSource).isInstanceOf(SharedInformerEventSource.class);
    }

    @Test
    void requireFreshStatusDropsEventsFromSecondariesWithStaleStatus() {
        // Given
        SharedInformerEventSource<KafkaService, VirtualKafkaCluster> eventSource = buildSharedEventSourceAlwaysMatchingAPrimary();
        EventHandler eventHandler = mock();
        eventSource.setEventHandler(eventHandler);
        KafkaService staleService = MapperTestSupport.buildKafkaService("svc", 2L, 1L);

        // When
        eventSource.onAdd(staleService);

        // Then
        verify(eventHandler, never()).handleEvent(any());
    }

    @Test
    void requireFreshStatusPropagatesEventsFromSecondariesWithFreshStatus() {
        // Given
        SharedInformerEventSource<KafkaService, VirtualKafkaCluster> eventSource = buildSharedEventSourceAlwaysMatchingAPrimary();
        EventHandler eventHandler = mock();
        eventSource.setEventHandler(eventHandler);
        KafkaService freshService = MapperTestSupport.buildKafkaService("svc", 1L, 1L);

        // When
        eventSource.onAdd(freshService);

        // Then
        verify(eventHandler).handleEvent(any());
    }

    @SuppressWarnings("unchecked")
    private static SharedInformerEventSource<KafkaService, VirtualKafkaCluster> buildSharedEventSourceAlwaysMatchingAPrimary() {
        EventSourceContext<VirtualKafkaCluster> context = mock();
        SharedInformerManager sharedInformerManager = mock();
        SharedIndexInformer<KafkaService> informer = mock();
        when(sharedInformerManager.getOrCreateInformer(KafkaService.class)).thenReturn(informer);
        when(sharedInformerManager.effectiveNamespaces()).thenReturn(Set.of());
        SecondaryToPrimaryMapper<KafkaService> alwaysMatchesAPrimary = secondary -> Set.of(new ResourceID("cluster", "ns"));

        EventSource<KafkaService, VirtualKafkaCluster> eventSource = EventSourceBuilder.forSecondary(context, KafkaService.class)
                .withName("services")
                .withSharedInformer(sharedInformerManager)
                .withMappers(NO_SECONDARIES, alwaysMatchesAPrimary)
                .requireFreshStatus()
                .build();
        return (SharedInformerEventSource<KafkaService, VirtualKafkaCluster>) eventSource;
    }
}
