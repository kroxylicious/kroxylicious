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

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;
import static io.kroxylicious.kubernetes.operator.checksum.MetadataChecksumGenerator.NO_CHECKSUM_SPECIFIED;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceStrimziKafkaRefReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceStrimziKafkaRefReconcilerIT.class);
    public static final String FOO_BOOTSTRAP_9090 = "foo.bootstrap:9090";
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
    void shouldUpdateStatusOnceStrimziKafkaResourceDeleted() {
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
