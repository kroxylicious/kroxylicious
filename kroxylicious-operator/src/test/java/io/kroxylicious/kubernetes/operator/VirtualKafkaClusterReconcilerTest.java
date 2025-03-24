/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaCluster;
import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.VirtualKafkaClusterStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VirtualKafkaClusterReconcilerTest {

    // @formatter:off
    public static final VirtualKafkaCluster CLUSTER = new VirtualKafkaClusterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
                .withNewProxyRef()
                    .withName("my-proxy")
                .endProxyRef()
                .addNewIngressRef()
                    .withName("my-ingress")
                .endIngressRef()
                .withNewTargetKafkaServiceRef()
                    .withName("my-kafka")
                .endTargetKafkaServiceRef()
            .endSpec()
            .build();

    public static final KafkaProxy PROXY = new KafkaProxyBuilder()
            .withNewMetadata()
                .withName("my-proxy")
                .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();
    // @formatter:on

    @Test
    void shouldSetResolvedRefsToFalseWhenProxyNotFound() {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);
        when(context.getSecondaryResource(KafkaProxy.class, VirtualKafkaClusterReconciler.PROXY_EVENT_SOURCE_NAME)).thenReturn(Optional.empty());

        // when
        var update = reconciler.reconcile(CLUSTER, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .isResolvedRefsFalse("spec.proxyRef.name", "KafkaProxy not found")
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToTrueWhenProxyFound() {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);
        when(context.getSecondaryResource(KafkaProxy.class, VirtualKafkaClusterReconciler.PROXY_EVENT_SOURCE_NAME)).thenReturn(Optional.of(PROXY));

        // when
        var update = reconciler.reconcile(CLUSTER, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .isResolvedRefsTrue()
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new VirtualKafkaClusterReconciler(z);

        Context<VirtualKafkaCluster> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(CLUSTER, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        VirtualKafkaClusterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(CLUSTER)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }
}
