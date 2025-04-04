/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

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

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaService;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceBuilder;
import io.kroxylicious.kubernetes.operator.assertj.KafkaServiceStatusAssert;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@EnableKubernetesMockClient(crud = true)
@ExtendWith(MockitoExtension.class)
class KafkaServiceReconcilerTest {

    public static final Clock TEST_CLOCK = Clock.fixed(Instant.EPOCH, ZoneId.of("Z"));

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
    void shouldSetAcceptedToUnknown() {
        // given
        final KafkaService kafkaService = new KafkaServiceBuilder().withNewMetadata().withGeneration(OBSERVED_GENERATION).endMetadata().build();

        var reconciler = new KafkaServiceReconciler(TEST_CLOCK);

        Context<KafkaService> context = mock(Context.class);

        // when
        var update = reconciler.updateErrorStatus(kafkaService, context, new RuntimeException("Boom!"));

        // then
        assertThat(update).isNotNull();
        assertThat(update.getResource()).isPresent();
        KafkaServiceStatusAssert.assertThat(update.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .singleCondition()
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .isAcceptedUnknown("java.lang.RuntimeException", "Boom!")
                .hasLastTransitionTime(TEST_CLOCK.instant());

    }

    @Test
    void shouldSetAcceptedToTrue() {
        // Given
        final KafkaService kafkaService = new KafkaServiceBuilder().withNewMetadata().withGeneration(OBSERVED_GENERATION).endMetadata().build();

        // When
        final UpdateControl<KafkaService> updateControl = kafkaProtocolFilterReconciler.reconcile(kafkaService, context);

        // Then
        assertThat(updateControl).isNotNull();
        assertThat(updateControl.getResource()).isPresent();
        KafkaServiceStatusAssert.assertThat(updateControl.getResource().get().getStatus())
                .hasObservedGenerationInSyncWithMetadataOf(kafkaService)
                .conditionList()
                .containsOnlyTypes(Condition.Type.Accepted)
                .singleOfType(Condition.Type.Accepted)
                .isAcceptedTrue();
    }
}
