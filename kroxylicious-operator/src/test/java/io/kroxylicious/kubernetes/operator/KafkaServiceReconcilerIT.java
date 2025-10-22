/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;
import java.util.List;

import org.assertj.core.api.Assertions;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;
import io.fabric8.kubernetes.api.model.Secret;
import io.fabric8.kubernetes.api.model.SecretBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.dsl.Updatable;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;
import io.strimzi.api.kafka.Crds;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.api.kafka.model.kafka.KafkaBuilder;
import io.strimzi.api.kafka.model.kafka.listener.GenericKafkaListenerBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerAddressBuilder;
import io.strimzi.api.kafka.model.kafka.listener.ListenerStatusBuilder;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;
import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceReconcilerIT.class);
    public static final String FOO_BOOTSTRAP_9090 = "foo.bootstrap:9090";
    private static final String BAR_BOOTSTRAP_9090 = "bar.bootstrap:9090";
    public static final String SERVICE_A = "service-a";
    public static final String SECRET_X = "secret-x";
    public static final String CONFIG_MAP_T = "configmap-t";
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
            .build();

    @BeforeAll
    static void beforeAll() {
        // note that we could not find a nice way to do this via the LocallyRunOperatorExtension. I tried serializing the CRD to
        // a temp file and using `withAdditionalCRD(path)` but it didn't load those CRDs before initializing the reconciler.
        try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
            client.apiextensions().v1().customResourceDefinitions().resource(Crds.kafka()).createOr(Updatable::update);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterAll
    static void afterAll() {
        try (KubernetesClient client = OperatorTestUtils.kubeClient()) {
            client.apiextensions().v1().customResourceDefinitions().resource(Crds.kafka()).delete();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldImmediatelyResolveWhenNoReferents() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, null, null);

        // When
        testActor.create(resource);

        // Then
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, false);
    }

    @Test
    void shouldResolveUpdateToKafkaService() {
        // Given
        var kafkaService = testActor.create(new KafkaServiceBuilder().withNewMetadata().withName(SERVICE_A).endMetadata().build());

        // When
        final KafkaService updated = kafkaService.edit().editSpec().withBootstrapServers(BAR_BOOTSTRAP_9090).endSpec().build();
        testActor.replace(updated);

        // Then
        assertResolvedRefsTrue(updated, BAR_BOOTSTRAP_9090, false);
    }

    @Test
    void shouldEventuallyResolveOnceCertSecretCreated() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, SECRET_X, null);

        // When
        final KafkaService kafkaService = testActor.create(resource);

        // Then
        assertResolvedRefsFalse(kafkaService, Condition.REASON_REFS_NOT_FOUND, "spec.tls.certificateRef: referenced secret not found");

        // And When
        testActor.create(tlsCertificateSecret(SECRET_X));

        // Then
        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, true);

    }

    @Test
    void shouldUpdateStatusOnceTlsCertificateSecretDeleted() {
        // Given
        var tlsCertSecret = testActor.create(tlsCertificateSecret(SECRET_X));
        KafkaService resource = testActor.create(kafkaService(SERVICE_A, SECRET_X, null));
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, true);

        // When
        testActor.delete(tlsCertSecret);

        // Then
        assertResolvedRefsFalse(resource, Condition.REASON_REFS_NOT_FOUND, "spec.tls.certificateRef: referenced secret not found");
    }

    @Test
    void shouldEventuallyResolveOnceTrustAnchorCreated() {
        // Given
        KafkaService resource = kafkaService(SERVICE_A, null, CONFIG_MAP_T);

        // When
        final KafkaService kafkaService = testActor.create(resource);

        // Then
        assertResolvedRefsFalse(kafkaService, Condition.REASON_REFS_NOT_FOUND, "spec.tls.trustAnchorRef: referenced configmap not found");

        // And When
        testActor.create(trustAnchorConfigMap(CONFIG_MAP_T));

        // Then
        assertResolvedRefsTrue(kafkaService, FOO_BOOTSTRAP_9090, true);

    }

    @Test
    void shouldUpdateReferentAnnotationWhenTrustAnchorConfigMapModified() {
        // Given
        testActor.create(trustAnchorConfigMap(CONFIG_MAP_T));
        KafkaService resource = kafkaService(SERVICE_A, null, CONFIG_MAP_T);
        final KafkaService kafkaService = testActor.create(resource);
        String checksum = awaitReferentsChecksumSpecified(resource);

        // When
        testActor.replace(trustAnchorConfigMap(CONFIG_MAP_T).edit().addToData("arbitrary", "arbitrary").build());

        // Then
        assertReferentsChecksumNotEqual(kafkaService, checksum);
    }

    @Test
    void shouldUpdateReferentAnnotationWhenCertificateSecretModified() {
        // Given
        testActor.create(tlsCertificateSecret(SECRET_X));
        KafkaService resource = testActor.create(kafkaService(SERVICE_A, SECRET_X, null));
        String checksum = awaitReferentsChecksumSpecified(resource);

        // When
        testActor.replace(tlsCertificateSecret(SECRET_X).edit().addToData("arbitrary", "whatever").build());

        // Then
        assertReferentsChecksumNotEqual(resource, checksum);
    }

    @Test
    void shouldUpdateStatusOnceTrustAnchorDeleted() {
        // Given
        var trustedCaCerts = testActor.create(trustAnchorConfigMap(CONFIG_MAP_T));
        KafkaService resource = testActor.create(kafkaService(SERVICE_A, null, CONFIG_MAP_T));
        assertResolvedRefsTrue(resource, FOO_BOOTSTRAP_9090, true);

        // When
        testActor.delete(trustedCaCerts);

        // Then
        assertResolvedRefsFalse(resource, Condition.REASON_REFS_NOT_FOUND, "spec.tls.trustAnchorRef: referenced configmap not found");
    }

    @Test
    void shouldResolveStrimziKafka() {
        // Given
        var kafka = testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));
        String listenerHost = "foo.bootstrap";
        int listenerPort = 9090;
        Kafka withStatus = new KafkaBuilder(kafka)
                .withNewStatus()
                .withListeners(
                        new ListenerStatusBuilder()
                                .withName("plain")
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(listenerHost)
                                        .withPort(listenerPort)
                                        .build())
                                .build())
                .endStatus()
                .build();
        testActor.patchStatus(withStatus);

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsTrue(service, FOO_BOOTSTRAP_9090, true);
    }

    @Test
    void shouldHandleStrimziKafkaWithNoPlainListeners() {
        // Given
        String listenerHost = "mylistener";
        int listenerPort = 9092;
        Kafka withTlsListener = new KafkaBuilder()
                .withNewMetadata()
                .withName(KAFKA_RESOURCE_NAME)
                .endMetadata()
                .withNewSpec()
                .withNewKafka()
                .withListeners(List.of(new GenericKafkaListenerBuilder()
                        .withName("tls")
                        .withTls(true)
                        .build()))
                .endKafka()
                .endSpec()
                .withNewStatus()
                .withListeners(
                        new ListenerStatusBuilder()
                                .withName("tls")
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(listenerHost)
                                        .withPort(listenerPort)
                                        .build())
                                .build())
                .endStatus()
                .build();

        testActor.create(withTlsListener);

        // When
        KafkaService service = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "tls", KAFKA_RESOURCE_NAME));

        // Then
        assertResolvedRefsFalse(service, Condition.REASON_INVALID_REFERENCED_RESOURCE, "Referenced resource should have listener as `plain`");
    }

    @Test
    void shouldHandleStrimziKafkaWithNoListeners() {
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
    void shouldUpdateStatusOnceKafkaResourceDeleted() {
        // Given
        var kafka = testActor.create(kafkaResource(KAFKA_RESOURCE_NAME));
        String listenerHost = "foo.bootstrap";
        int listenerPort = 9090;
        Kafka withListeners = new KafkaBuilder(kafka)
                .withNewStatus()
                .withListeners(
                        new ListenerStatusBuilder()
                                .withName("plain")
                                .withAddresses(new ListenerAddressBuilder()
                                        .withHost(listenerHost)
                                        .withPort(listenerPort)
                                        .build())
                                .build())
                .endStatus()
                .build();
        testActor.patchStatus(withListeners);

        // When
        KafkaService updated = testActor.create(kafkaServiceWithStrimziKafkaRef(SERVICE_A, "plain", KAFKA_RESOURCE_NAME));
        assertResolvedRefsTrue(updated, FOO_BOOTSTRAP_9090, true);

        // When
        testActor.delete(kafka);

        // Then
        assertResolvedRefsFalse(updated, Condition.REASON_REFS_NOT_FOUND, "spec.strimziKafkaRef: referenced Kafka resource not found");
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

    private static KafkaService kafkaService(String name, @Nullable String certSecretName, @Nullable String trustResourceName) {
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
            final KafkaService kafkaService = testActor.get(KafkaService.class, ResourcesUtil.name(cr));
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
            final KafkaService kafkaService = testActor.get(KafkaService.class, ResourcesUtil.name(cr));
            return getReferentChecksum(kafkaService);
        }, s -> !s.equals(NO_CHECKSUM_SPECIFIED));
    }

    private void assertReferentsChecksumNotEqual(KafkaService cr, String checksum) {
        AWAIT.untilAsserted(() -> {
            final KafkaService kafkaService = testActor.get(KafkaService.class, ResourcesUtil.name(cr));
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
}
