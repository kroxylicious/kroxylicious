/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.time.Instant;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.common.ConditionBuilder;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressBuilder;

class ConditionAssertTest {

    public static final Instant BASE_TIME = Instant.now();
    public static final long BASE_GENERATION = 9871345L;
    public static final String READY_MESSAGE = "Lets rock and roll";
    public static final String READY_REASON = "AllGoodBro";
    private Condition readyCondition;
    private Condition failedRefsCondition;
    private Condition passedRefsCondition;
    private Condition acceptedCondition;
    private Condition resolvedRefsUnknownCondition;
    private Condition resolvedRefsTrueCondition;

    @BeforeEach
    void setUp() {
        readyCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Ready)
                .withMessage(READY_MESSAGE)
                .withReason(READY_REASON)
                .build();

        failedRefsCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage(READY_MESSAGE)
                .withReason(READY_REASON)
                .build();

        passedRefsCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withMessage(READY_MESSAGE)
                .withReason(READY_REASON)
                .build();

        acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.TRUE)
                .withMessage("")
                .withReason("Accepted")
                .build();
        resolvedRefsUnknownCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.UNKNOWN)
                .withMessage("Its naw there like")
                .withReason("NotFound")
                .build();
        resolvedRefsTrueCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withMessage("")
                .withReason("ResolvedRefs")
                .build();
    }

    @Test
    void shouldThrowForMissMatchedGenerations() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasObservedGeneration(BASE_GENERATION + 1));
    }

    @Test
    void shouldPassForMatchedGenerations() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(readyCondition).hasObservedGeneration(BASE_GENERATION);
    }

    @Test
    void shouldThrowForMissMatchedTypes() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasType(Condition.Type.Accepted));
    }

    @Test
    void shouldPassForMatchedTypes() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(readyCondition).hasType(Condition.Type.Ready);
    }

    @Test
    void shouldThrowForMissMatchedMessages() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasMessage("But if you try sometime you'll find\n"
                + "You get what you need"));
    }

    @Test
    void shouldPassForMatchedMessages() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(readyCondition).hasMessage(READY_MESSAGE);
    }

    @Test
    void shouldThrowForMissMatchedNoMessages() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasNoMessage());
    }

    @Test
    void shouldPassForMatchedNoMessages() {
        // Given
        Condition conditionWithoutMessage = readyCondition.edit().withMessage("").build();
        // When
        // Then
        ConditionAssert.assertThat(conditionWithoutMessage).hasNoMessage();
    }

    @Test
    void shouldThrowForMissMatchedReasons() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasReason("OK_COMPUTER"));
    }

    @Test
    void shouldPassForMatchedReasons() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(readyCondition).hasReason(READY_REASON);
    }

    @Test
    void shouldThrowForMissMatchedStatus() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(failedRefsCondition).hasStatus(Condition.Status.TRUE));
    }

    @Test
    void shouldPassForMatchedStatus() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(failedRefsCondition).hasStatus(Condition.Status.FALSE);
    }

    @Test
    void shouldThrowForMissMatchedLastTransitionTime() {
        // Given
        // When

        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(failedRefsCondition).hasLastTransitionTime(BASE_TIME.plusSeconds(30)));
    }

    @Test
    void shouldPassForMatchedLastTransitionTime() {
        // Given
        // When

        // Then
        ConditionAssert.assertThat(failedRefsCondition).hasLastTransitionTime(BASE_TIME);
    }

    @Test
    void shouldThrowForOutOfSyncGenerations() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("foo").withGeneration(BASE_GENERATION - 193954L).endMetadata()
                .build();
        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).hasObservedGenerationInSyncWithMetadataOf(thingWithMetadata));
    }

    @Test
    void shouldPassInSyncGenerations() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("foo").withGeneration(BASE_GENERATION).endMetadata()
                .build();

        // When

        // Then
        ConditionAssert.assertThat(readyCondition).hasObservedGenerationInSyncWithMetadataOf(thingWithMetadata);
    }

    @Test
    void shouldFailAcceptedTrueForReadyCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).isAcceptedTrue());
    }

    @Test
    void shouldFailAcceptedTrueForResolvedRefsCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(passedRefsCondition).isAcceptedTrue());
    }

    @Test
    void shouldPassAcceptedTrueForAcceptedCondition() {
        // Given
        var acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.TRUE)
                .withMessage("")
                .withReason("Accepted")
                .build();
        // When
        ConditionAssert.assertThat(acceptedCondition).isAcceptedTrue();

        // Then
    }

    @Test
    void shouldFailAcceptedTrueForResolvedRefsConditionOnHasMetadata() {
        // Given
        HasMetadata thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withGeneration(7890L).endMetadata().build();

        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(acceptedCondition).isAcceptedTrue(thingWithMetadata));
    }

    @Test
    void shouldPassAcceptedTrueForAcceptedConditionOnHasMetadata() {
        // Given
        HasMetadata thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withGeneration(acceptedCondition.getObservedGeneration()).endMetadata().build();

        // When
        ConditionAssert.assertThat(acceptedCondition).isAcceptedTrue(thingWithMetadata);

        // Then
    }

    @Test
    void shouldFailAcceptedFalseForReadyCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).isAcceptedFalse(READY_REASON, READY_MESSAGE));
    }

    @Test
    void shouldFailAcceptedFalseForResolvedRefsCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> ConditionAssert.assertThat(passedRefsCondition).isAcceptedFalse(passedRefsCondition.getReason(), passedRefsCondition.getMessage()));
    }

    @Test
    void shouldPassAcceptedFalseForAcceptedCondition() {
        // Given
        var acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.FALSE)
                .withMessage("Its false like")
                .withReason("NotFound")
                .build();

        // When
        ConditionAssert.assertThat(acceptedCondition).isAcceptedFalse("NotFound", "Its false like");
        // Then
    }

    @Test
    void shouldFailAcceptedFalseForWrongReason() {
        // Given
        var acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.FALSE)
                .withMessage("Its false like")
                .withReason("NotFound")
                .build();

        // When
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(acceptedCondition).isAcceptedFalse(READY_REASON, "Its false like"));
        // Then
    }

    @Test
    void shouldFailAcceptedFalseForWrongMessage() {
        // Given
        var acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.FALSE)
                .withMessage("Its false like")
                .withReason("NotFound")
                .build();

        // When
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(acceptedCondition).isAcceptedFalse("NotFound", "an other message"));
        // Then
    }

    @Test
    void shouldFailResolvedRefsUnknownFalseForReadyCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(readyCondition).isResolvedRefsUnknown(READY_REASON, READY_MESSAGE));
    }

    @Test
    void shouldFailResolvedRefsUnknownFalseForResolvedRefsCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> ConditionAssert.assertThat(passedRefsCondition).isResolvedRefsUnknown(passedRefsCondition.getReason(), passedRefsCondition.getMessage()));
    }

    @Test
    void shouldPassResolvedRefsUnknownFalseForResolvedRefsUnknownCondition() {
        // Given

        // When
        ConditionAssert.assertThat(resolvedRefsUnknownCondition).isResolvedRefsUnknown("NotFound", "Its naw there like");
        // Then
    }

    @Test
    void shouldFailResolvedRefsUnknownFalseForWrongReason() {
        // Given
        var resolvedRefsUnknownCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage("Its false like")
                .withReason("NotFound")
                .build();

        // When
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(resolvedRefsUnknownCondition).isResolvedRefsUnknown(READY_REASON, "Its false like"));
        // Then
    }

    @Test
    void shouldFailResolvedRefsUnknownFalseForWrongMessage() {
        // Given
        var resolvedRefsUnknownCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.FALSE)
                .withMessage("Its false like")
                .withReason("NotFound")
                .build();

        // When
        Assertions.assertThatThrownBy(() -> ConditionAssert.assertThat(resolvedRefsUnknownCondition).isResolvedRefsFalse("NotFound", "an other message"));
        // Then
    }

    @Test
    void shouldPassResolvedRefsTrueFalseForResolvedRefsTrueCondition() {
        // Given

        // When
        ConditionAssert.assertThat(resolvedRefsTrueCondition).isResolvedRefsTrue();
        // Then
    }

    @Test
    void shouldFailResolvedRefsTrueForResolvedRefsTrueConditionWithMessage() {
        // Given
        var resolvedRefsTrueCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.ResolvedRefs)
                .withStatus(Condition.Status.TRUE)
                .withMessage("blah")
                .withReason("ResolvedRefs")
                .build();

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> ConditionAssert.assertThat(resolvedRefsTrueCondition).isResolvedRefsUnknown(passedRefsCondition.getReason(), passedRefsCondition.getMessage()));
    }

    @Test
    void shouldPassResolvedRefsTrueForThingWithMetadata() {
        // Given
        var thingWithMetadata = new KafkaProxyIngressBuilder().withNewMetadata().withName("foo").withGeneration(BASE_GENERATION).endMetadata()
                .build();

        // When

        // Then
        ConditionAssert.assertThat(resolvedRefsTrueCondition).isResolvedRefsTrue(thingWithMetadata);
    }

    @Test
    void shouldFailResolvedRefsTrueForResolvedRefsWithWrongReason() {
        // Given
        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> ConditionAssert.assertThat(resolvedRefsTrueCondition).isResolvedRefsUnknown(passedRefsCondition.getReason(),
                        resolvedRefsTrueCondition.getMessage()));
    }

    @Test
    void shouldFailResolvedRefsTrueForResolvedRefsUnknownCondition() {
        // Given
        var unknownRefsCondition =
                // When
                // Then
                Assertions.assertThatThrownBy(
                        () -> ConditionAssert.assertThat(resolvedRefsUnknownCondition).isResolvedRefsUnknown(passedRefsCondition.getReason(),
                                passedRefsCondition.getMessage()));
    }

    @Test
    void shouldFailResolvedRefsTrueForResolvedRefsFalseCondition() {
        // Given

        // When
        // Then
        Assertions.assertThatThrownBy(
                () -> ConditionAssert.assertThat(failedRefsCondition).isResolvedRefsUnknown(passedRefsCondition.getReason(), passedRefsCondition.getMessage()));
    }

    @Test
    void shouldPassIsReadyUnknownForReadyUnknownCondition() {
        // Given
        var readyCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Ready)
                .withStatus(Condition.Status.UNKNOWN)
                .withMessage("blah")
                .withReason("ResolvedRefs")
                .build();

        // When

        // Then
        ConditionAssert.assertThat(readyCondition).isReadyUnknown(readyCondition.getReason(), readyCondition.getMessage());
    }

    @Test
    void shouldPassIsReadyFalseForReadyFalseCondition() {
        // Given
        var readyCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Ready)
                .withStatus(Condition.Status.FALSE)
                .withMessage("blah")
                .withReason("ResolvedRefs")
                .build();

        // When

        // Then
        ConditionAssert.assertThat(readyCondition).isReadyFalse(readyCondition.getReason(), readyCondition.getMessage());
    }

    @Test
    void shouldPassIsReadyTrueForReadyTrueCondition() {
        // Given
        var readyCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Ready)
                .withStatus(Condition.Status.TRUE)
                .withMessage("")
                .withReason("Ready")
                .build();

        // When

        // Then
        ConditionAssert.assertThat(readyCondition).isReadyTrue();
    }

    @Test
    void shouldPassIsAcceptedUnknownForAcceptedUnknownCondition() {
        // Given
        var acceptedCondition = new ConditionBuilder()
                .withLastTransitionTime(BASE_TIME)
                .withObservedGeneration(BASE_GENERATION)
                .withType(Condition.Type.Accepted)
                .withStatus(Condition.Status.UNKNOWN)
                .withMessage("")
                .withReason("Ready")
                .build();

        // When

        // Then
        ConditionAssert.assertThat(acceptedCondition).isAcceptedUnknown(acceptedCondition.getReason(), acceptedCondition.getMessage());
    }
}