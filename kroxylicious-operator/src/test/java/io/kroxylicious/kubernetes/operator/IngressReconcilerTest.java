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

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngress;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProxyIngressStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IngressReconcilerTest {

    public static final KafkaProxyIngress INGRESS = new KafkaProxyIngressBuilder().editOrNewMetadata()
            .withName("foo")
            .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
            .withNewProxyRef()
            .withName("my-proxy")
            .endProxyRef()
            .endSpec()
            .build();

    public static final KafkaProxy PROXY = new KafkaProxyBuilder().withNewMetadata()
            .withName("my-proxy")
            .withGeneration(101L)
            .endMetadata()
            .withNewSpec()
            .endSpec()
            .build();

    @Test
    void shouldSetResolvedRefsToFalseWhenProxyNotFound() throws Exception {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new IngressReconciler(z);

        Context<KafkaProxyIngress> context = mock(Context.class);
        when(context.getSecondaryResource(eq(KafkaProxy.class), eq(""))).thenReturn(Optional.empty());

        // when
        var update = reconciler.reconcile(INGRESS, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        KafkaProxyIngressStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(INGRESS)
                .singleCondition()
                .hasType(Condition.Type.ResolvedRefs)
                .hasStatus(Condition.Status.FALSE)
                .hasReason("spec.proxyRef.name")
                .hasMessage("KafkaProxy not found")
                .lastTransitionTimeIsEqualTo(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToTrueWhenProxyFound() throws Exception {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new IngressReconciler(z);

        Context<KafkaProxyIngress> context = mock(Context.class);
        when(context.getSecondaryResource(eq(KafkaProxy.class), eq(IngressReconciler.PROXY_EVENT_SOURCE_NAME))).thenReturn(Optional.of(PROXY));

        // when
        var update = reconciler.reconcile(INGRESS, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        KafkaProxyIngressStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(INGRESS)
                .singleCondition()
                .hasType(Condition.Type.ResolvedRefs)
                .hasStatus(Condition.Status.TRUE)
                .hasNoReason()
                .hasNoMessage()
                .lastTransitionTimeIsEqualTo(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToUnknown() throws Exception {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new IngressReconciler(z);

        Context<KafkaProxyIngress> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(INGRESS, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        KafkaProxyIngressStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(INGRESS)
                .singleCondition()
                .hasType(Condition.Type.ResolvedRefs)
                .hasStatus(Condition.Status.UNKNOWN)
                .hasReason("java.lang.RuntimeException")
                .hasMessage("Boom!")
                .lastTransitionTimeIsEqualTo(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }
}
