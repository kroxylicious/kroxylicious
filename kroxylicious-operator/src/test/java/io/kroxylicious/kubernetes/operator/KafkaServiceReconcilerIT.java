/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Duration;

import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.javaoperatorsdk.operator.junit.LocallyRunOperatorExtension;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions;
import io.kroxylicious.kubernetes.operator.kafkaservice.KafkaServiceReconciler;

import static org.awaitility.Awaitility.await;

@EnabledIf(value = "io.kroxylicious.kubernetes.operator.OperatorTestUtils#isKubeClientAvailable", disabledReason = "no viable kube client available")
class KafkaServiceReconcilerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaServiceReconcilerIT.class);

    private KubernetesClient client;
    private static final ConditionFactory AWAIT = await().timeout(Duration.ofSeconds(60));

    @BeforeEach
    void beforeEach() {
        client = OperatorTestUtils.kubeClient();
    }

    @SuppressWarnings("JUnitMalformedDeclaration")
    @RegisterExtension
    LocallyRunOperatorExtension extension = LocallyRunOperatorExtension.builder()
            .withReconciler(new KafkaServiceReconciler(Clock.systemUTC()))
            .withKubernetesClient(client)
            .waitForNamespaceDeletion(true)
            .withConfigurationService(x -> x.withCloseClientOnStop(false))
            .build();

    @AfterEach
    void stopOperator() {
        extension.getOperator().stop();
        LOGGER.atInfo().log("Test finished");
    }

    @Test
    void shouldAcceptKafkaService() {
        // Given

        // When
        extension.create(new KafkaServiceBuilder().withNewMetadata().withName("mycoolkafkaservice").endMetadata().build());

        // Then
        AWAIT.untilAsserted(() -> {
            final KafkaService mycoolkafkaservice = extension.get(KafkaService.class, "mycoolkafkaservice");
            OperatorAssertions.assertThat(mycoolkafkaservice.getStatus())
                    .isNotNull()
                    .singleCondition()
                    .isAcceptedTrue();
        });
    }
}
