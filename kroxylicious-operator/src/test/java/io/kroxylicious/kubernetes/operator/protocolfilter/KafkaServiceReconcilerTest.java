/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.protocolfilter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.server.mock.EnableKubernetesMockClient;
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer;
import io.javaoperatorsdk.operator.api.reconciler.Context;
import io.javaoperatorsdk.operator.api.reconciler.UpdateControl;

import java.time.Clock;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.kafkaservice.KafkaServiceReconciler;

import static io.kroxylicious.kubernetes.operator.assertj.OperatorAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
class KafkaServiceReconcilerTest {

    public static final long OBSERVED_GENERATION = 1345L;
    KubernetesClient kubeClient;
    KubernetesMockServer mockServer;

    @Mock
    Context<KafkaService> context;

    private KafkaServiceReconciler kafkaProtocolFilterReconciler;

    @BeforeEach
    void setUp() {
        kafkaProtocolFilterReconciler = new KafkaServiceReconciler(Clock.systemUTC());
    }

    @Test
    void shouldMarkFilterAccepted() {
        // Given
        final KafkaService kafkaService = new KafkaServiceBuilder().withNewMetadata().withGeneration(OBSERVED_GENERATION).endMetadata().build();

        // When
        final UpdateControl<KafkaService> updateControl = kafkaProtocolFilterReconciler.reconcile(kafkaService, context);

        // Then
        assertThat(updateControl)
                .isNotNull()
                .satisfies(
                        input -> {
                            assertThat(input.isPatchStatus())
                                    .isTrue();
                            assertThat(input.getResource())
                                    .isNotEmpty()
                                    .get()
                                    .extracting(KafkaService::getStatus)
                                    .satisfies(kafkaServiceStatus -> {
                                        assertThat(kafkaServiceStatus)
                                                .observedGeneration()
                                                .isEqualTo(OBSERVED_GENERATION);
                                        assertThat(kafkaServiceStatus)
                                                .singleCondition()
                                                .hasObservedGenerationInSyncWithMetadataOf(input.getResource().get())
                                                .isAcceptedTrue();
                                    }

                                    );
                        });
    }
}