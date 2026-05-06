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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRef;
import io.kroxylicious.kubernetes.api.common.TrustAnchorRefBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaservicestatus.Tls;
import io.kroxylicious.kubernetes.operator.Annotations;
import io.kroxylicious.kubernetes.operator.LocallyRunningOperatorRbacHandler;
import io.kroxylicious.kubernetes.operator.ResourcesUtil;
import io.kroxylicious.kubernetes.operator.TestFiles;
import io.kroxylicious.testing.operator.assertj.OperatorAssertions;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.ResourcesUtil.STRIMZI_CLUSTER_CA_BUNDLE;
import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED;
import static io.kroxylicious.testing.operator.assertj.OperatorAssertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceStrimziKafkaRefReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceStrimziKafkaRefReconcilerIT.class);
    public static final String FOO_BOOTSTRAP = "foo.bootstrap";
    public static final int FOO_BOOTSTRAP_PORT = 9090;
    public static final String FOO_BOOTSTRAP_9090 = FOO_BOOTSTRAP + ":" + FOO_BOOTSTRAP_PORT;
    public static final String SERVICE_A = "service-a";
    public static final String KAFKA_RESOURCE_NAME = "my-cluster";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler(TestFiles.INSTALL_MANIFESTS_DIR,
            "*.ClusterRole.kroxylicious-operator-watched.yaml");

    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC()))
            .withKubernetesClient(rbacHandler.operatorClient())
            .waitForNamespaceDeletion(false)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .withAdditionalCustomResourceDefinition(Crds.kafka())
            .build();

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldResolveStrimziKafkaWithPlainListener() {
        // Given
        var kafka = testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
    }

    @Test
    void shouldResolveStrimziKafkaWithTlsListener() {
        // Given
        var kafka = testActor.create(kafkaResourceWithTls(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "tls", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
    }

    @Test
    void shouldHandleStrimziKafkaWithNoReconciledListeners() {
        // Given
        var kafka = testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));

        Kafka withNoListener = new KafkaBuilder(kafka)
                .withNewStatus()
                .endStatus()
                .build();

        testActor.patchStatus(withNoListener);

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsFalse(service, Condition.REASON_REFERENCED_RESOURCE_NOT_RECONCILED, "Referenced resource has not yet reconciled listener name: plain");
    }

    @Test
    void shouldHandleStrimziKafkaWithNoStatus() {
        // Given
        testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsFalse(service, Condition.REASON_REFERENCED_RESOURCE_NOT_RECONCILED, "Referenced resource has not yet reconciled listener name: plain");
    }

    @Test
    void shouldUpdateStatusOnceStrimziKafkaResourceDeleted() {
        // Given
        var kafka = testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);

        // When
        KafkaService updated = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));
        assertResolvedRefsTrue(updated, FOO_BOOTSTRAP_9090, true);

        // When
        testActor.delete(kafka);

        // Then
        assertResolvedRefsFalse(updated, Condition.REASON_REFS_NOT_FOUND, "spec.strimziKafkaRef: referenced Kafka resource not found");
    }

    /**
     * Tests that when trustAnchorRef is present, it always takes precedence
     * regardless of trustStrimziCaCertificate setting.
     */
    @Test
    void trustAnchorRefAlwaysTakesPrecedenceWhenPresent() {
        // Given
        var kafka = testActor.create(kafkaResourceWithTls(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);
        createTrustAnchorSecret("explicit-trust", "ca-bundle.pem");

        var tar = createTrustAnchorRef("explicit-trust", "Secret");

        // When - Create KafkaService with trustAnchorRef AND trustStrimziCaCertificate=true
        var service = testActor.create(kafkaServiceWithStrimziKafkaRefAndOptionalTrustAnchor("service-with-both-refs", "tls", true, tar));

        // Then - Explicit trustAnchorRef should be used (not Strimzi CA)
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
        assertKafkaService("service-with-both-refs", "explicit-trust", "Secret");
    }

    /**
     * Tests that trustAnchorRef is used even when trustStrimziCaCertificate is explicitly false.
     */
    @Test
    void trustAnchorRefUsedWhenTrustStrimziCaDisabled() {
        // Given
        var kafka = testActor.create(kafkaResourceWithTls(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);
        createTrustAnchorConfigMap("explicit-trust");

        var tar = createTrustAnchorRef("explicit-trust", "ConfigMap");

        // When - Create KafkaService with trustAnchorRef AND trustStrimziCaCertificate=false
        var service = testActor.create(kafkaServiceWithStrimziKafkaRefAndOptionalTrustAnchor("service-trust-anchor-only", "tls", false, tar));

        // Then - Explicit trustAnchorRef should be used
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
        assertKafkaService("service-trust-anchor-only", "explicit-trust", "ConfigMap");
    }

    /**
     * Tests that when trustAnchorRef is NOT present and trustStrimziCaCertificate is enabled,
     * the Strimzi CA is used.
     */
    @Test
    void strimziCaUsedWhenTrustAnchorRefNotPresent() {
        // Given
        var kafka = testActor.create(kafkaResourceWithTls(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);
        String clusterCaSecretName = KAFKA_RESOURCE_NAME + ResourcesUtil.STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX;
        createStrimziTrustAnchorSecret(clusterCaSecretName);

        // When - Create KafkaService with trustStrimziCaCertificate=true but NO trustAnchorRef
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRefAndOptionalTrustAnchor("service-strimzi-only", "tls", true, null));

        // Then - Strimzi CA should be used
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
        assertKafkaService("service-strimzi-only", clusterCaSecretName, "Secret");

    }

    /**
     * Tests that when spec.tls is completely null but trustStrimziCaCertificate is enabled,
     * status.tls is correctly built with the auto-discovered trust anchor and all other
     * TLS fields are null (demonstrating null-safe handling in the builder).
     */
    @Test
    void shouldBuildStatusTlsWhenSpecTlsIsNullButTrustAnchorRefDiscovered() {
        // Given
        var kafka = testActor.create(kafkaResourceWithTls(KAFKA_RESOURCE_NAME));
        reconcileStrimziResource(kafka);
        String clusterCaSecretName = KAFKA_RESOURCE_NAME + ResourcesUtil.STRIMZI_CLUSTER_CA_CERT_SECRET_SUFFIX;
        createStrimziTrustAnchorSecret(clusterCaSecretName);

        // When - Create KafkaService with spec.tls=null and trustStrimziCaCertificate=true
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRefAndNullTls("service-null-tls", "tls"));

        // Then - status.tls should be built with Strimzi CA as trust anchor
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
        AWAIT.untilAsserted(() -> {
            var kafkaService = testActor.get(KafkaService.class, "service-null-tls");
            assertThat(kafkaService)
                    .isNotNull()
                    .extracting(KafkaService::getStatus)
                    .isNotNull()
                    .extracting(KafkaServiceStatus::getTls)
                    .isNotNull();
            var tls = kafkaService.getStatus().getTls();
            Assertions.assertThat(tls.getTrustAnchorRef()).isNotNull();
            Assertions.assertThat(tls.getTrustAnchorRef().getRef().getName()).isEqualTo(clusterCaSecretName);
            Assertions.assertThat(tls.getTrustAnchorRef().getRef().getKind()).isEqualTo("Secret");
            Assertions.assertThat(tls.getTrustAnchorRef().getKey()).isEqualTo(STRIMZI_CLUSTER_CA_BUNDLE);
            Assertions.assertThat(tls.getTrustAnchorRef().getStoreType()).isEqualTo("PEM");
            // Validate null-safe handling - these should all be null since spec.tls was null
            Assertions.assertThat(tls.getCertificateRef()).isNull();
            Assertions.assertThat(tls.getProtocols()).isNull();
            Assertions.assertThat(tls.getCipherSuites()).isNull();
        });
    }

    private ConfigMap createTrustAnchorConfigMap(String name) {
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .addToData("ca-bundle.pem", "whatever")
                .build();
        testActor.create(configMap);
        return configMap;
    }

    private Secret createTrustAnchorSecret(String name, String key) {
        Secret secret = new SecretBuilder()
                .withNewMetadata()
                .withName(name)
                .endMetadata()
                .addToData(key, "whatever")
                .build();
        testActor.create(secret);
        return secret;
    }

    private Secret createStrimziTrustAnchorSecret(String name) {
        return createTrustAnchorSecret(name, STRIMZI_CLUSTER_CA_BUNDLE);
    }

    private KafkaService kafkaServiceWithStrimziKafkaRefAndOptionalTrustAnchor(String serviceName,
                                                                               String listenerName,
                                                                               boolean trustStrimziCa,
                                                                               @Nullable TrustAnchorRef explictTrust) {
        // @formatter:off
        var builder = new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(serviceName)
                .endMetadata()
                .editOrNewSpec()
                    .withNewStrimziKafkaRef()
                        .withListenerName(listenerName)
                        .withTrustStrimziCaCertificate(trustStrimziCa)
                        .withNewRef()
                            .withName(KAFKA_RESOURCE_NAME)
                        .endRef()
                    .endStrimziKafkaRef();

        if (explictTrust != null) {
            builder.withNewTls()
                    .withTrustAnchorRef(explictTrust)
                .endTls();
        }

        return builder.endSpec().build();
        // @formatter:on
    }

    private Kafka kafkaResource(String resourceName) {
        // @formatter:off
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .withListeners(new GenericKafkaListenerBuilder()
                                .withName("plain")
                                .withTls(false)
                                .build())
                    .endKafka()
                .endSpec()
                .build();
        // @formatter:on
    }

    private Kafka kafkaResourceWithTls(String resourceName) {
        // @formatter:off
        return new KafkaBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                .endMetadata()
                .withNewSpec()
                    .withNewKafka()
                        .addNewListener()
                            .withName("tls")
                            .withTls(true)
                        .endListener()
                    .endKafka()
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaServiceWithStrimziKafkaRef(String resourceName, String listenerName, String clusterName) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                .endMetadata()
                .editOrNewSpec()
                    .withNewStrimziKafkaRef()
                    .withListenerName(listenerName)
                    .withNewRef()
                        .withName(clusterName)
                     .endRef()
                    .endStrimziKafkaRef()
                .endSpec()
                .build();
        // @formatter:on
    }

    private static KafkaService kafkaServiceWithStrimziKafkaRefAndNullTls(String resourceName, String listenerName) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(resourceName)
                .endMetadata()
                .editOrNewSpec()
                    .withNewStrimziKafkaRef()
                        .withListenerName(listenerName)
                        .withTrustStrimziCaCertificate(true)
                        .withNewRef()
                            .withName(KAFKA_RESOURCE_NAME)
                        .endRef()
                    .endStrimziKafkaRef()
                    .withTls(null)
                .endSpec()
                .build();
        // @formatter:on
    }

    private void assertResolvedRefsTrue(KafkaService cr, String expectedBootstrap, boolean hasReferents) {
        AWAIT.untilAsserted(() -> {
            final KafkaService kafkaService = testActor.get(KafkaService.class, ResourcesUtil.name(cr));
            Assertions.assertThat(kafkaService).isNotNull();
            assertThat(kafkaService.getStatus())
                    .isNotNull()
                    .satisfies(status -> {
                        Assertions.assertThat(status.getBootstrapServers()).isEqualTo(expectedBootstrap);
                        assertThat(status)
                                .conditionList()
                                .singleElement()
                                .isResolvedRefsTrue();
                    });
            String checksum = getReferentChecksum(kafkaService);
            if (hasReferents) {
                Assertions.assertThat(checksum).isNotEqualTo(NO_CHECKSUM_SPECIFIED);
            }
            else {
                Assertions.assertThat(checksum).isEqualTo(NO_CHECKSUM_SPECIFIED);
            }
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
            var kafkaService = testActor.resources(KafkaService.class)
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

    private void assertKafkaService(String kafkaServiceName, String expectedResourceName, String expectedResourceType) {
        AWAIT.untilAsserted(() -> {
            var kafkaService = testActor.get(KafkaService.class, kafkaServiceName);
            assertThat(kafkaService)
                    .isNotNull()
                    .extracting(KafkaService::getStatus)
                    .isNotNull()
                    .extracting(KafkaServiceStatus::getTls)
                    .isNotNull();
            var tls = kafkaService.getStatus().getTls();
            Assertions.assertThat(tls)
                    .extracting(Tls::getTrustAnchorRef)
                    .satisfies(tar -> {
                        Assertions.assertThat(tar.getRef().getName()).isEqualTo(expectedResourceName);
                        Assertions.assertThat(tar.getRef().getKind()).isEqualTo(expectedResourceType);
                    });
        });
    }

    // simulates what the Strimzi operator would do
    private void reconcileStrimziResource(Kafka kafka) {

        // @formatter:off
        var statusListeners= kafka.getSpec().getKafka().getListeners().stream().map(specListener ->
                new ListenerStatusBuilder()
                        .withName(specListener.getName())
                        .addNewAddress()
                            .withHost(FOO_BOOTSTRAP)
                            .withPort(FOO_BOOTSTRAP_PORT)
                        .endAddress()
                .build()).toList();
        // @formatter:on

        // @formatter:off
        testActor.patchStatus(new KafkaBuilder(kafka)
                .withNewStatus()
                    .addAllToListeners(statusListeners)
                .endStatus()
            .build());
        // @formatter:on
    }

    private TrustAnchorRef createTrustAnchorRef(String name, String kind) {
        // @formatter:off
        return new TrustAnchorRefBuilder()
                    .withNewRef()
                        .withName(name)
                      .withKind(kind)
                    .endRef()
                    .withKey("ca-bundle.pem")
                .build();
        // @formatter:off
    }

}
