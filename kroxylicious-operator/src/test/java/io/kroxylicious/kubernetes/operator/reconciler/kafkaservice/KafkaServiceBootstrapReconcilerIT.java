/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.reconciler.kafkaservice;

import java.time.Clock;
import java.time.Duration;

import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.LocalKroxyliciousOperatorExtension;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;
import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceBootstrapReconcilerIT {

    public static final String FOO_BOOTSTRAP_9090 = "foo.bootstrap:9090";
    private static final String BAR_BOOTSTRAP_9090 = "bar.bootstrap:9090";
    public static final String SERVICE_A = "service-a";
    public static final String SECRET_X = "secret-x";
    public static final String SECRET_T = "secret-t";
    public static final String CONFIG_MAP_T = "configmap-t";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocalKroxyliciousOperatorExtension operator = LocalKroxyliciousOperatorExtension.builder()
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC()))
            .replaceClusterRoleGlobs("*.ClusterRole.kroxylicious-operator-watched.yaml")
            .build();

    @Test
    void shouldImmediatelyResolveWhenNoReferents() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, null, null, null);

        // When
        operator.create(resource);

        // Then
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, false);
    }

    @Test
    void shouldResolveUpdateToKafkaService() {
        // Given
        var kafkaService = operator.create(
                new KafkaServiceBuilder().withNewMetadata().withName(SERVICE_A).endMetadata().withNewSpec().withBootstrapServers(FOO_BOOTSTRAP_9090).endSpec().build());

        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, false);

        // When
        final KafkaService updated = kafkaService.edit().editSpec().withBootstrapServers(BAR_BOOTSTRAP_9090).endSpec().build();
        operator.replace(updated);

        // Then
        assertResolvedRefsTrue(kafkaService, BAR_BOOTSTRAP_9090, false);
    }

    @Test
    void shouldEventuallyResolveOnceCertSecretCreated() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, SECRET_X, null, null);

        // When
        final KafkaService kafkaService = operator.create(resource);

        // Then
        assertResolvedRefsFalse(kafkaService, Condition.REASON_REFS_NOT_FOUND, "spec.tls.certificateRef: referenced secret not found");

        // And When
        operator.create(tlsCertificateSecret(SECRET_X));

        // Then
        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, true);

    }

    @Test
    void shouldUpdateStatusOnceTlsCertificateSecretDeleted() {
        // Given
        var tlsCertSecret = operator.create(tlsCertificateSecret(SECRET_X));
        KafkaService resource = operator.create(kafkaService(SERVICE_A, SECRET_X, null, null));
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, true);

        // When
        operator.delete(tlsCertSecret);

        // Then
        assertResolvedRefsFalse(resource, Condition.REASON_REFS_NOT_FOUND, "spec.tls.certificateRef: referenced secret not found");
    }

    @Test
    void shouldEventuallyResolveOnceTrustAnchorConfigMapCreated() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, null, CONFIG_MAP_T, null);

        // When
        final KafkaService kafkaService = operator.create(resource);

        // Then
        assertResolvedRefsFalse(kafkaService, Condition.REASON_REFS_NOT_FOUND, "spec.tls.trustAnchorRef: referenced configmap not found");

        // And When
        operator.create(trustAnchorConfigMap(CONFIG_MAP_T));

        // Then
        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, true);

    }

    @Test
    void shouldEventuallyResolveOnceTrustAnchorSecretCreated() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, null, SECRET_T, "Secret");

        // When
        final KafkaService kafkaService = operator.create(resource);

        // Then
        assertResolvedRefsFalse(kafkaService, Condition.REASON_REFS_NOT_FOUND, "spec.tls.trustAnchorRef: referenced secret not found");

        // And When
        operator.create(trustAnchorSecret(SECRET_T));

        // Then
        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, true);

    }

    @Test
    void shouldUpdateReferentAnnotationWhenTrustAnchorConfigMapModified() {
        // Given
        operator.create(trustAnchorConfigMap(CONFIG_MAP_T));
        KafkaService resource = kafkaService(SERVICE_A, null, CONFIG_MAP_T, null);
        final KafkaService kafkaService = operator.create(resource);
        String checksum = awaitReferentsChecksumSpecified(resource);

        // When
        operator.replace(trustAnchorConfigMap(CONFIG_MAP_T).edit().addToData("arbitrary", "arbitrary").build());

        // Then
        assertReferentsChecksumNotEqual(kafkaService, checksum);
    }

    @Test
    void shouldUpdateReferentAnnotationWhenCertificateSecretModified() {
        // Given
        operator.create(tlsCertificateSecret(SECRET_X));
        KafkaService resource = operator.create(kafkaService(SERVICE_A, SECRET_X, null, null));
        String checksum = awaitReferentsChecksumSpecified(resource);

        // When
        operator.replace(tlsCertificateSecret(SECRET_X).edit().addToData("arbitrary", "whatever").build());

        // Then
        assertReferentsChecksumNotEqual(resource, checksum);
    }

    @Test
    void shouldUpdateStatusOnceTrustAnchorConfigMapDeleted() {
        // Given
        var trustedCaCerts = operator.create(trustAnchorConfigMap(CONFIG_MAP_T));
        KafkaService resource = operator.create(kafkaService(SERVICE_A, null, CONFIG_MAP_T, null));
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, true);

        // When
        operator.delete(trustedCaCerts);

        // Then
        assertResolvedRefsFalse(resource, Condition.REASON_REFS_NOT_FOUND, "spec.tls.trustAnchorRef: referenced configmap not found");
    }

    private ConfigMap trustAnchorConfigMap(String name) {
        // @formatter:off
        return new ConfigMapBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .addToData("ca-bundle.pem", "whatever")
                .build();
        // @formatter:on
    }

    private Secret trustAnchorSecret(String name) {
        // @formatter:off
        return new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .addToData("ca-bundle.pem", "whatever")
                .build();
        // @formatter:on
    }

    private static Secret tlsCertificateSecret(String name) {
        // @formatter:off
        return new SecretBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .withType("kubernetes.io/tls")
                .addToData("tls.key", "whatever")
                .addToData("tls.crt", "whatever")
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaService(String name, @Nullable String certSecretName, @Nullable String trustResourceName, @Nullable String trustResourceKind) {
        // @formatter:off
        KafkaService result = new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers(FOO_BOOTSTRAP_9090)
                .endSpec()
            .build();
        if (certSecretName != null) {
            result = new KafkaServiceBuilder(result)
                    .editSpec()
                        .editOrNewTls()
                            .withNewCertificateRef()
                                .withName(certSecretName)
                            .endCertificateRef()
                        .endTls()
                    .endSpec()
                .build();
        }
        if (trustResourceName != null) {
            result = new KafkaServiceBuilder(result)
                    .editSpec()
                        .editOrNewTls()
                            .withNewTrustAnchorRef()
                                .withNewRef()
                                    .withName(trustResourceName)
                                    .withKind(trustResourceKind)
                                .endRef()
                                .withKey("ca-bundle.pem")
                            .endTrustAnchorRef()
                        .endTls()
                    .endSpec()
                .build();
        }
        return result;
        // @formatter:on
    }

    private void assertResolvedRefsTrue(KafkaService cr, String expectedBootstrap, boolean hasReferents) {
        AWAIT.untilAsserted(() -> {
            final KafkaService kafkaService = operator.get(KafkaService.class, ResourcesUtil.name(cr));
            Assertions.assertThat(kafkaService).isNotNull();
            Assertions.assertThat(kafkaService.getStatus().getBootstrapServers()).isEqualTo(expectedBootstrap);
            assertThat(kafkaService.getStatus())
                    .isNotNull()
                    .conditionList()
                    .singleElement()
                    .isResolvedRefsTrue();
            String checksum = getReferentChecksum(kafkaService);
            if (hasReferents) {
                Assertions.assertThat(checksum).isNotEqualTo(NO_CHECKSUM_SPECIFIED);
            }
            else {
                Assertions.assertThat(checksum).isEqualTo(NO_CHECKSUM_SPECIFIED);
            }
        });
    }

    private String awaitReferentsChecksumSpecified(KafkaService cr) {
        return AWAIT.until(() -> {
            final KafkaService kafkaService = operator.get(KafkaService.class, ResourcesUtil.name(cr));
            return getReferentChecksum(kafkaService);
        }, s -> !s.equals(NO_CHECKSUM_SPECIFIED));
    }

    private void assertReferentsChecksumNotEqual(KafkaService cr, String checksum) {
        AWAIT.untilAsserted(() -> {
            final KafkaService kafkaService = operator.get(KafkaService.class, ResourcesUtil.name(cr));
            String actualChecksum = getReferentChecksum(kafkaService);
            Assertions.assertThat(actualChecksum).isNotEqualTo(checksum);
        });
    }

    private static String getReferentChecksum(KafkaService kafkaService) {
        return kafkaService.getMetadata().getAnnotations()
                .getOrDefault(Annotations.REFERENT_CHECKSUM_ANNOTATION_KEY, NO_CHECKSUM_SPECIFIED);
    }

    private void assertResolvedRefsFalse(KafkaService cr,
                                         String reason,
                                         String message) {
        AWAIT.alias("KafkaServiceStatusResolvedRefs").untilAsserted(() -> {
            var kafkaService = operator.resources(KafkaService.class)
                    .withName(ResourcesUtil.name(cr)).get();
            Assertions.assertThat(kafkaService.getStatus()).isNotNull();
            OperatorAssertions
                    .assertThat(kafkaService.getStatus())
                    .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                    .singleCondition()
                    .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                    .isResolvedRefsFalse(reason, message);
        });
    }
}
