/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaprotocolfilter;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.util.Base64;
import java.util.Map;
import java.util.Set;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilter;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterBuilder;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.SecureConfigInterpolator;
import io.kroxylicious.kubernetes.operator.informer.SharedInformerManager;
import io.kroxylicious.testing.operator.ClusterUser;
import io.kroxylicious.testing.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.testing.operator.OperatorTestUtils;
import io.kroxylicious.testing.operator.assertj.KafkaProtocolFilterStatusAssert;

import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED;
import static io.kroxylicious.testing.operator.OperatorTestUtils.uniqueSuffix;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.testing.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
@SuppressWarnings("java:S8692") // ITs run against a live API server; a fixed clock would be misleading since time is not controlled
class KafkaProtocolFilterReconcilerIT {

    private static final String A = "a";
    private static final String B = "b";
    private static final String C = "c";
    private static final String FILTER_ONE = "one";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    private static final SharedInformerManager sharedInformerManager = new SharedInformerManager(OperatorTestUtils.kubeClient(), Set.of());

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new KafkaProtocolFilterReconciler(Clock.systemUTC(), SecureConfigInterpolator.DEFAULT_INTERPOLATOR, sharedInformerManager))
            .replaceClusterRoleGlobs("*.ClusterRole.kroxylicious-operator-watched.yaml")
            .build();

    private ClusterUser clusterUser;

    @BeforeEach
    void setUp() {
        clusterUser = operator.clusterUser();
    }

    @Test
    void shouldEventuallyResolveWhenFilterCreatedFirst() {
        var suffix = uniqueSuffix();
        createFilterFirst(suffix);
    }

    private KafkaProtocolFilter createFilterFirst(String suffix) {
        KafkaProtocolFilter filterOne = clusterUser.create(filter(FILTER_ONE + suffix,
                "${secret:" + (A + suffix) + ":foo}", "${configmap:" + (B + suffix) + ":foo}"));
        assertResolvedRefsFalse(filterOne, "Referenced Secrets [" + (A + suffix) + "] ConfigMaps [" + (B + suffix) + "] not found");
        clusterUser.create(secret(A + suffix));
        assertResolvedRefsFalse(filterOne, "Referenced ConfigMaps [" + (B + suffix) + "] not found");
        clusterUser.create(cm(B + suffix));
        assertAllConditionsTrue(filterOne);
        return filterOne;
    }

    @Test
    void shouldEventuallyResolveWhenASecretCreatedFirst() {
        var suffix = uniqueSuffix();
        clusterUser.create(secret(A + suffix));
        KafkaProtocolFilter filterOne = clusterUser.create(filter(FILTER_ONE + suffix,
                "${secret:" + (A + suffix) + ":foo}", "${secret:" + (B + suffix) + ":foo}"));
        assertResolvedRefsFalse(filterOne, "Referenced Secrets [" + (B + suffix) + "] not found");
        clusterUser.create(secret(B + suffix));
        assertAllConditionsTrue(filterOne);
    }

    @Test
    void shouldEventuallyResolveWhenAllSecretsCreatedFirst() {
        var suffix = uniqueSuffix();
        clusterUser.create(secret(A + suffix));
        clusterUser.create(secret(B + suffix));
        clusterUser.create(secret(C + suffix));
        KafkaProtocolFilter filterOne = clusterUser.create(filter(FILTER_ONE + suffix,
                "${secret:" + (A + suffix) + ":foo}",
                "${secret:" + (B + suffix) + ":foo}"));
        assertAllConditionsTrue(filterOne);
    }

    private void assertAllConditionsTrue(KafkaProtocolFilter filterOne) {
        AWAIT.alias("FilterStatusResolvedRefs").untilAsserted(() -> {
            var kpf = clusterUser.resources(KafkaProtocolFilter.class)
                    .withName(ResourcesUtil.name(filterOne)).get();
            assertThat(kpf.getStatus()).isNotNull();
            KafkaProtocolFilterStatusAssert
                    .assertThat(kpf.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpf)
                    .conditionList()
                    .singleElement()
                    .isResolvedRefsTrue(kpf);
            String checksum = kpf.getMetadata().getAnnotations()
                    .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);
            assertThat(checksum).isNotEqualTo(NO_CHECKSUM_SPECIFIED);
        });
    }

    @Test
    void shouldEventuallyResolveWhenSecretsAndConfigMapsFirst() {
        var suffix = uniqueSuffix();
        clusterUser.create(secret(A + suffix));
        clusterUser.create(cm(B + suffix));
        clusterUser.create(secret(C + suffix));
        KafkaProtocolFilter filterOne = clusterUser.create(filter(FILTER_ONE + suffix,
                "${secret:" + (A + suffix) + ":foo}",
                "${configmap:" + (B + suffix) + ":foo}"));
        assertAllConditionsTrue(filterOne);
    }

    @Test
    void shouldUpdateStatusOnFilterModify() {
        var suffix = uniqueSuffix();
        createFilterFirst(suffix);

        KafkaProtocolFilter filterOne = clusterUser.replace(filter(FILTER_ONE + suffix,
                "${secret:" + (C + suffix) + ":foo}", "${configmap:" + (B + suffix) + ":foo}"));
        assertResolvedRefsFalse(filterOne, "Referenced Secrets [" + (C + suffix) + "] not found");

        clusterUser.create(secret(C + suffix));
        assertAllConditionsTrue(filterOne);
    }

    @Test
    void shouldUpdateStatusOnSecretModify() {
        var suffix = uniqueSuffix();
        var filterOne = createFilterFirst(suffix);

        clusterUser.resources(Secret.class).withName(A + suffix).edit(secret -> secret.edit()
                .addToData("baz", Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)))
                .build());
        assertAllConditionsTrue(filterOne);
    }

    @Test
    void shouldUpdateReferentAnnotationOnSecretModify() {
        // given
        var suffix = uniqueSuffix();
        var filterOne = createFilterFirst(suffix);
        String checksum = clusterUser.get(KafkaProtocolFilter.class, ResourcesUtil.name(filterOne)).getMetadata().getAnnotations()
                .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);

        // when
        clusterUser.resources(Secret.class).withName(A + suffix).edit(secret -> secret.edit()
                .addToData("baz", Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)))
                .build());

        // then
        // Editing the referent doesn't change the filter's metadata.generation, so an observedGeneration check
        // alone is satisfied by the pre-edit state. Poll until the referent checksum changes (the reliable signal
        // that the referent-triggered reconciliation completed) while confirming the conditions still hold (see #4018).
        AWAIT.alias("referent checksum updated and conditions still true").untilAsserted(() -> {
            var kpf = clusterUser.get(KafkaProtocolFilter.class, ResourcesUtil.name(filterOne));
            assertThat(kpf.getStatus()).isNotNull();
            KafkaProtocolFilterStatusAssert
                    .assertThat(kpf.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpf)
                    .conditionList()
                    .singleElement()
                    .isResolvedRefsTrue(kpf);
            String newChecksum = kpf.getMetadata().getAnnotations()
                    .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);
            assertThat(newChecksum).isNotEqualTo(checksum);
        });
    }

    @Test
    void shouldUpdateReferentAnnotationOnConfigMapModify() {
        // given
        var suffix = uniqueSuffix();
        var filterOne = createFilterFirst(suffix);
        String checksum = clusterUser.get(KafkaProtocolFilter.class, ResourcesUtil.name(filterOne)).getMetadata().getAnnotations()
                .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);

        // when
        clusterUser.resources(ConfigMap.class).withName(B + suffix).edit(configMap -> configMap.edit()
                .addToData("baz", Base64.getEncoder().encodeToString("".getBytes(StandardCharsets.UTF_8)))
                .build());

        // then
        // Editing the referent doesn't change the filter's metadata.generation, so an observedGeneration check
        // alone is satisfied by the pre-edit state. Poll until the referent checksum changes (the reliable signal
        // that the referent-triggered reconciliation completed) while confirming the conditions still hold (see #4018).
        AWAIT.alias("referent checksum updated and conditions still true").untilAsserted(() -> {
            var kpf = clusterUser.get(KafkaProtocolFilter.class, ResourcesUtil.name(filterOne));
            assertThat(kpf.getStatus()).isNotNull();
            KafkaProtocolFilterStatusAssert
                    .assertThat(kpf.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpf)
                    .conditionList()
                    .singleElement()
                    .isResolvedRefsTrue(kpf);
            String newChecksum = kpf.getMetadata().getAnnotations()
                    .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);
            assertThat(newChecksum).isNotEqualTo(checksum);
        });
    }

    @Test
    void shouldUpdateStatusOnSecretDelete() {
        var suffix = uniqueSuffix();
        var filterOne = createFilterFirst(suffix);

        clusterUser.delete(secret(A + suffix));
        assertResolvedRefsFalse(filterOne, "Referenced Secrets [" + (A + suffix) + "] not found");
    }

    @Test
    void shouldUpdateStatusOnConfigMapDelete() {
        var suffix = uniqueSuffix();
        var filterOne = createFilterFirst(suffix);

        clusterUser.delete(cm(B + suffix));
        assertResolvedRefsFalse(filterOne, "Referenced ConfigMaps [" + (B + suffix) + "] not found");
    }

    private KafkaProtocolFilter filter(String filterName, String refA, String refB) {
        // @formatter:off
        return new KafkaProtocolFilterBuilder()
                .withNewMetadata()
                    .withName(filterName)
                .endMetadata()
                .withNewSpec()
                    .withType("org.example.Filter")
                    .withConfigTemplate(Map.of(
                            "normalProp", "normalValue",
                            "securePropA", refA,
                            "securePropB", refB))
                .endSpec()
                .build();
        // @formatter:on
    }

    private void assertResolvedRefsFalse(KafkaProtocolFilter cr,
                                         String message) {
        AWAIT.alias("FilterStatusResolvedRefs").untilAsserted(() -> {
            var kpf = clusterUser.resources(KafkaProtocolFilter.class)
                    .withName(ResourcesUtil.name(cr)).get();
            assertThat(kpf.getStatus()).isNotNull();
            KafkaProtocolFilterStatusAssert
                    .assertThat(kpf.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kpf)
                    .singleCondition()
                    .hasType(Condition.Type.ResolvedRefs)
                    .hasStatus(Condition.Status.FALSE)
                    .hasMessage(message)
                    .hasObservedGenerationInSyncWithMetadataOf(kpf);
        });
    }

    Secret secret(String name) {
        // @formatter:off
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .addToData("foo", Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)))
                .build();
        // @formatter:on
    }

    ConfigMap cm(String name) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .addToData("foo", "bar")
                .build();
        // @formatter:on
    }

}
