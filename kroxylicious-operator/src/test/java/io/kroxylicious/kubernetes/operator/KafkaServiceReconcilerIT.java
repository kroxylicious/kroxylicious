/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

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

import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;
import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceReconcilerIT.class);
    private static final String UPDATED_BOOTSTRAP = "bar.bootstrap:9090";
    public static final String SERVICE_A = "service-a";

    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @RegisterExtension
    static LocallyRunningOperatorRbacHandler rbacHandler = new LocallyRunningOperatorRbacHandler("install", "*.ClusterRole.kroxylicious-operator-watched.yaml");

    @SuppressWarnings("JUnitMalformedDeclaration") // The beforeAll and beforeEach have the same effect so we can use it as an instance field.
    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC()))
            .withKubernetesClient(rbacHandler.operatorClient())
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    private final LocallyRunningOperatorRbacHandler.TestActor testActor = rbacHandler.testActor(extension);

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldAcceptKafkaService() {
        // Given
        final KafkaService kafkaService = testActor.create(kafkaService(SERVICE_A));
        final KafkaService updated = kafkaService.edit().editSpec().withBootstrapServers(UPDATED_BOOTSTRAP).endSpec().build();

        // When
        testActor.replace(updated);

        // Then
        AWAIT.untilAsserted(() -> {
            final KafkaService mycoolkafkaservice = testActor.get(KafkaService.class, SERVICE_A);
            Assertions.assertThat(mycoolkafkaservice.getSpec().getBootstrapServers()).isEqualTo(UPDATED_BOOTSTRAP);
            assertThat(mycoolkafkaservice.getStatus())
                    .isNotNull()
                    .singleCondition()
                    .hasObservedGenerationInSyncWithMetadataOf(mycoolkafkaservice)
                    .isAcceptedTrue();
        });
    }

    @Test
    void shouldAcceptUpdateToKafkaService() {
        // Given

        // When
        testActor.create(new KafkaServiceBuilder().withNewMetadata().withName(SERVICE_A).endMetadata().build());

        // Then
        AWAIT.untilAsserted(() -> {
            final KafkaService mycoolkafkaservice = testActor.get(KafkaService.class, SERVICE_A);
            assertThat(mycoolkafkaservice.getStatus())
                    .isNotNull()
                    .singleCondition()
                    .hasObservedGenerationInSyncWithMetadataOf(mycoolkafkaservice)
                    .isAcceptedTrue();
        });
    }

    private static KafkaService kafkaService(String name) {
        // @formatter:off
        return new KafkaServiceBuilder()
                .withNewMetadata()
                    .withName(name)
                .endMetadata()
                .editOrNewSpec()
                    .withBootstrapServers("foo.bootstrap:9090")
                .endSpec()
                .build();
        // @formatter:on
    }
}
