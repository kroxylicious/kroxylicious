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
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProtocolFilterStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProtocolFilterReconcilerTest {

    // @formatter:off
    public static final KafkaProtocolFilter FILTER = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
            .endMetadata()
            .withNewSpec()
                .withType("org.example.MyFilter")
                .withConfigTemplate(Map.of(
                        "normalProp", "normalValue",
                        "securePropA", "${secret:my-secret:key}",
                        "securePropB", "${configmap:my-configmap:key}"))
            .endSpec()
            .build();

    public static final Secret SECRET = new SecretBuilder()
            .withNewMetadata()
            .withName("my-secret")
            .withGeneration(42L)
            .endMetadata()
            .addToData("key", "value")
            .build();

    public static final ConfigMap CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
            .withName("my-configmap")
            .withGeneration(42L)
            .endMetadata()
            .addToData("key", "value")
            .build();
    // @formatter:on

    @Test
    void shouldSetResolvedRefsToFalseWhenReferentsNotFound() throws Exception {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new KafkaProtocolFilterReconciler(z, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        Context<KafkaProtocolFilter> context = mock(Context.class);
        when(context.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of());
        when(context.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of());

        // when
        var update = reconciler.reconcile(FILTER, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        KafkaProtocolFilterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .isResolvedRefsFalse("MissingInterpolationReferences", "Referenced Secrets [my-secret] ConfigMaps [my-configmap] not found")
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToTrueWhenReferentsFound() throws Exception {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new KafkaProtocolFilterReconciler(z, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        Context<KafkaProtocolFilter> context = mock(Context.class);
        when(context.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(SECRET));
        when(context.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(CONFIG_MAP));

        // when
        var update = reconciler.reconcile(FILTER, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        KafkaProtocolFilterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .isResolvedRefsTrue()
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        Clock z = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));
        var reconciler = new KafkaProtocolFilterReconciler(z, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        Context<KafkaProtocolFilter> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(FILTER, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        KafkaProtocolFilterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .isResolvedRefsUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(ZonedDateTime.ofInstant(z.instant(), z.getZone()));

    }
}
