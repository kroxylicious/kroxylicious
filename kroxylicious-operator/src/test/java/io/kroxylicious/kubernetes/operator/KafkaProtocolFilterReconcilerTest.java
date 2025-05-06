/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.api.reconciler.Context;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.assertj.ConditionListAssert;
import io.kroxylicious.kubernetes.operator.assertj.KafkaProtocolFilterStatusAssert;

import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.REFERENT_CHECKSUM_ANNOTATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class KafkaProtocolFilterReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

    // @formatter:off
    public static final KafkaProtocolFilter FILTER = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
                .withUid("foo-uid")
            .endMetadata()
            .withNewSpec()
                .withType("org.example.MyFilter")
                .withConfigTemplate(Map.of(
                        "normalProp", "normalValue",
                        "securePropA", "${secret:my-secret:key}",
                        "securePropB", "${configmap:my-configmap:key}"))
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_MULTIPLE = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
                .withName("foo")
                .withGeneration(42L)
                .withUid("foo-uid")
            .endMetadata()
            .withNewSpec()
                .withType("org.example.MyFilter")
                .withConfigTemplate(Map.of(
                        "normalProp", "normalValue",
                        "securePropA", "${secret:my-secret:key}",
                        "securePropB", "${secret:my-secret2:key}",
                        "securePropC", "${configmap:my-configmap:key}",
                        "securePropD", "${configmap:my-configmap2:key}"))
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_SECRET_ONLY = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
            .withName("foo")
            .withGeneration(42L)
            .withUid("foo-uid")
            .endMetadata()
            .withNewSpec()
            .withType("org.example.MyFilter")
            .withConfigTemplate(Map.of(
                    "normalProp", "normalValue",
                    "securePropA", "${secret:my-secret:key}"))
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_CONFIGMAP_ONLY = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
            .withName("foo")
            .withGeneration(42L)
            .withUid("foo-uid")
            .endMetadata()
            .withNewSpec()
            .withType("org.example.MyFilter")
            .withConfigTemplate(Map.of(
                    "normalProp", "normalValue",
                    "securePropB", "${configmap:my-configmap:key}"))
            .endSpec()
            .build();

    public static final KafkaProtocolFilter FILTER_NO_REFERENTS = new KafkaProtocolFilterBuilder()
            .withNewMetadata()
            .withName("foo")
            .withGeneration(42L)
            .withUid("foo-uid")
            .endMetadata()
            .withNewSpec()
            .withType("org.example.MyFilter")
            .withConfigTemplate(Map.of("normalProp", "normalValue"))
            .endSpec()
            .build();

    public static final Secret SECRET = new SecretBuilder()
            .withNewMetadata()
                .withName("my-secret")
                .withResourceVersion("1024")
                .withUid("my-secret-uid")
            .endMetadata()
            .addToData("key", "value")
            .build();

    public static final Secret SECRET2 = new SecretBuilder()
            .withNewMetadata()
                .withName("my-secret2")
                .withResourceVersion("1048")
                .withUid("my-secret-uid2")
            .endMetadata()
            .addToData("key", "value")
            .build();

    public static final ConfigMap CONFIG_MAP = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap")
                .withResourceVersion("1064")
                .withUid("my-configmap-uid")
            .endMetadata()
            .addToData("key", "value")
            .build();

    public static final ConfigMap CONFIG_MAP2 = new ConfigMapBuilder()
            .withNewMetadata()
                .withName("my-configmap2")
                .withResourceVersion("1074")
                .withUid("my-configmap-uid2")
            .endMetadata()
            .addToData("key", "value")
            .build();
    // @formatter:on

    public static List<Arguments> shouldSetResolvedRefs() {
        Context<KafkaProtocolFilter> bothExist = mock(Context.class);
        when(bothExist.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(SECRET));
        when(bothExist.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(CONFIG_MAP));

        Context<KafkaProtocolFilter> secretExists = mock(Context.class);
        when(secretExists.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of(SECRET));
        when(secretExists.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of());

        Context<KafkaProtocolFilter> cmExists = mock(Context.class);
        when(cmExists.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of());
        when(cmExists.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of(CONFIG_MAP));

        Context<KafkaProtocolFilter> neitherExists = mock(Context.class);
        when(neitherExists.getSecondaryResourcesAsStream(Secret.class)).thenReturn(Stream.of());
        when(neitherExists.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(Stream.of());
        return List.of(
                Arguments.argumentSet("both exist", bothExist,
                        (Consumer<ConditionListAssert>) conditionList -> conditionList
                                .singleElement()
                                .isResolvedRefsTrue()),
                Arguments.argumentSet("secret exists", secretExists,
                        (Consumer<ConditionListAssert>) conditionList -> conditionList
                                .singleOfType(Condition.Type.ResolvedRefs)
                                .hasStatus(Condition.Status.FALSE)
                                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                                .hasLastTransitionTime(TEST_CLOCK.instant())
                                .hasReason(Condition.REASON_INTERPOLATED_REFS_NOT_FOUND)
                                .hasMessage("Referenced ConfigMaps [my-configmap] not found")),
                Arguments.argumentSet("configmap exists", cmExists,
                        (Consumer<ConditionListAssert>) conditionList -> conditionList
                                .singleOfType(Condition.Type.ResolvedRefs)
                                .hasStatus(Condition.Status.FALSE)
                                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                                .hasLastTransitionTime(TEST_CLOCK.instant())
                                .hasReason(Condition.REASON_INTERPOLATED_REFS_NOT_FOUND)
                                .hasMessage("Referenced Secrets [my-secret] not found")),
                Arguments.argumentSet("neither exists", neitherExists,
                        (Consumer<ConditionListAssert>) conditionList -> conditionList
                                .singleOfType(Condition.Type.ResolvedRefs)
                                .hasStatus(Condition.Status.FALSE)
                                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                                .hasLastTransitionTime(TEST_CLOCK.instant())
                                .hasReason(Condition.REASON_INTERPOLATED_REFS_NOT_FOUND)
                                .hasMessage("Referenced Secrets [my-secret] ConfigMaps [my-configmap] not found")));
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetResolvedRefs(Context<KafkaProtocolFilter> context, Consumer<ConditionListAssert> asserter) {
        // given

        var reconciler = new KafkaProtocolFilterReconciler(TEST_CLOCK, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        // when
        var update = reconciler.reconcile(FILTER, context);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchResourceAndStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        var x = KafkaProtocolFilterStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(FILTER)
                .conditionList();

        asserter.accept(x);

    }

    // todo when we have a Checksum component, rather than a static method, we should instead use a mock to check what was passed to the checksummer
    // we aren't really interested in the checksum string, rather that we deterministically pass the right resources in the right order to the checksummer
    public static Stream<Arguments> shouldSetReferentsChecksumAnnotation() {
        return Stream.of(Arguments.argumentSet("single secret, single configmap", Set.of(SECRET), Set.of(CONFIG_MAP), FILTER, "AAAAAPpoNcw"),
                Arguments.argumentSet("multiple secrets, multiple configmaps", Set.of(SECRET, SECRET2), Set.of(CONFIG_MAP, CONFIG_MAP2), FILTER_MULTIPLE, "AAAAAHlcgl4"),
                Arguments.argumentSet("configmap only", Set.of(), Set.of(CONFIG_MAP), FILTER_CONFIGMAP_ONLY, "AAAAAAkyYyY"),
                Arguments.argumentSet("secret only", Set.of(SECRET), Set.of(), FILTER_SECRET_ONLY, "AAAAAAfh3oQ"));
    }

    @ParameterizedTest
    @MethodSource
    void shouldSetReferentsChecksumAnnotation(Set<Secret> secretsInContext, Set<ConfigMap> configMapsInContext, KafkaProtocolFilter filter, String expectedChecksum) {
        // given
        Context<KafkaProtocolFilter> bothExist = mock(Context.class);
        when(bothExist.getSecondaryResourcesAsStream(Secret.class)).thenReturn(secretsInContext.stream());
        when(bothExist.getSecondaryResourcesAsStream(ConfigMap.class)).thenReturn(configMapsInContext.stream());
        var reconciler = new KafkaProtocolFilterReconciler(TEST_CLOCK, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        // when
        var update = reconciler.reconcile(filter, bothExist);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchResourceAndStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        assertThat(update.getResource().get().getMetadata().getAnnotations()).containsEntry(REFERENT_CHECKSUM_ANNOTATION, expectedChecksum);
    }

    @Test
    void shouldNotSetReferentsChecksumAnnotationWhenNoReferents() {
        // given
        Context<KafkaProtocolFilter> bothExist = mock(Context.class);
        when(bothExist.getSecondaryResources(Secret.class)).thenReturn(Set.of());
        when(bothExist.getSecondaryResources(ConfigMap.class)).thenReturn(Set.of(CONFIG_MAP));
        var reconciler = new KafkaProtocolFilterReconciler(TEST_CLOCK, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

        // when
        var update = reconciler.reconcile(FILTER_NO_REFERENTS, bothExist);

        // then
        assertThat(update).isNotNull();
        assertThat(update.isPatchResourceAndStatus()).isTrue();
        assertThat(update.getResource()).isPresent();
        assertThat(update.getResource().get().getMetadata().getAnnotations()).doesNotContainKey(REFERENT_CHECKSUM_ANNOTATION);
    }

    @Test
    void shouldSetResolvedRefsToUnknown() {
        // given
        var reconciler = new KafkaProtocolFilterReconciler(TEST_CLOCK, SecureConfigInterpolator.DEFAULT_INTERPOLATOR);

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
                .hasLastTransitionTime(TEST_CLOCK.instant());

    }
}
