/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.time.Instant;

import org.assertj.core.api.AbstractComparableAssert;
import org.assertj.core.api.AbstractInstantAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.ThrowingConsumer;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;

/**
 * Assertions on a status {@link Condition}.
 */
@SuppressWarnings("UnusedReturnValue")
public class ConditionAssert extends AbstractObjectAssert<ConditionAssert, Condition> {
    /**
     * Creates an assertion on the given condition.
     *
     * @param o the condition to assert on
     */
    protected ConditionAssert(
                              Condition o) {
        super(o, ConditionAssert.class);
    }

    /**
     * Creates an assertion on the given condition.
     *
     * @param actual the condition to assert on
     * @return a new assertion
     */
    public static ConditionAssert assertThat(Condition actual) {
        return new ConditionAssert(actual);
    }

    /**
     * Returns an assertion on the condition's observed generation.
     *
     * @return an assertion on the observed generation
     */
    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    /**
     * Asserts that the condition has the given observed generation.
     *
     * @param expected the expected observed generation
     * @return this assertion
     */
    public ConditionAssert hasObservedGeneration(Long expected) {
        observedGeneration().isEqualTo(expected);
        return this;
    }

    /**
     * Asserts that the condition's observed generation equals the metadata generation of the given resource.
     *
     * @param thing the resource whose metadata generation the condition should reflect
     * @return this assertion
     */
    public ConditionAssert hasObservedGenerationInSyncWithMetadataOf(HasMetadata thing) {
        hasObservedGeneration(thing.getMetadata().getGeneration());
        return this;
    }

    /**
     * Returns an assertion on the condition's type.
     *
     * @return an assertion on the type
     */
    public AbstractComparableAssert<?, Condition.Type> type() {
        return Assertions.assertThat(actual.getType());
    }

    /**
     * Asserts that the condition has the given type.
     *
     * @param expected the expected type
     * @return this assertion
     */
    public ConditionAssert hasType(Condition.Type expected) {
        type().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    /**
     * Returns an assertion on the condition's message.
     *
     * @return an assertion on the message
     */
    public AbstractStringAssert<?> message() {
        return Assertions.assertThat(actual.getMessage());
    }

    /**
     * Asserts that the condition has an empty message.
     *
     * @return this assertion
     */
    public ConditionAssert hasNoMessage() {
        message().isEmpty();
        return this;
    }

    /**
     * Asserts that the condition has the given message.
     *
     * @param expected the expected message
     * @return this assertion
     */
    public ConditionAssert hasMessage(String expected) {
        message().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    /**
     * Asserts that the condition's message satisfies the given requirements.
     *
     * @param expected the requirements the message must satisfy
     * @return this assertion
     */
    public ConditionAssert hasMessage(ThrowingConsumer<String> expected) {
        message().describedAs(actual.toString()).satisfies(expected);
        return this;
    }

    /**
     * Returns an assertion on the condition's reason.
     *
     * @return an assertion on the reason
     */
    public AbstractStringAssert<?> reason() {
        return Assertions.assertThat(actual.getReason());
    }

    /**
     * Asserts that the condition has the given reason.
     *
     * @param expected the expected reason
     * @return this assertion
     */
    public ConditionAssert hasReason(String expected) {
        reason().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    /**
     * Returns an assertion on the condition's status.
     *
     * @return an assertion on the status
     */
    public ObjectAssert<Condition.Status> status() {
        return Assertions.assertThat(actual.getStatus()).asInstanceOf(InstanceOfAssertFactories.type(Condition.Status.class));
    }

    /**
     * Asserts that the condition has the given status.
     *
     * @param expected the expected status
     * @return this assertion
     */
    public ConditionAssert hasStatus(Condition.Status expected) {
        status().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    /**
     * Returns an assertion on the condition's last transition time.
     *
     * @return an assertion on the last transition time
     */
    public AbstractInstantAssert<?> lastTransitionTime() {
        return Assertions.assertThat(actual.getLastTransitionTime());
    }

    /**
     * Asserts that the condition has the given last transition time.
     *
     * @param time the expected last transition time
     * @return this assertion
     */
    public ConditionAssert hasLastTransitionTime(Instant time) {
        lastTransitionTime().isEqualTo(time);
        return this;
    }

    /**
     * Asserts that this is an {@code Accepted} condition with status {@code True},
     * the standard reason and no message.
     *
     * @return this assertion
     */
    public ConditionAssert isAcceptedTrue() {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.Accepted.name());
        hasNoMessage();
        return this;
    }

    /**
     * Asserts that this is an {@code Accepted} condition with status {@code True},
     * the standard reason, no message, and an observed generation in sync with the given resource.
     *
     * @param thing the resource whose metadata generation the condition should reflect
     * @return this assertion
     */
    public ConditionAssert isAcceptedTrue(HasMetadata thing) {
        isAcceptedTrue()
                .hasObservedGenerationInSyncWithMetadataOf(thing);
        return this;
    }

    /**
     * Asserts that this is an {@code Accepted} condition with status {@code False}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param message the expected message
     * @return this assertion
     */
    public ConditionAssert isAcceptedFalse(String reason, String message) {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    /**
     * Asserts that this is a {@code ResolvedRefs} condition with status {@code Unknown}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param message the expected message
     * @return this assertion
     */
    public ConditionAssert isResolvedRefsUnknown(String reason, String message) {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    /**
     * Asserts that this is a {@code ResolvedRefs} condition with status {@code False}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param expectedMessage the expected message
     * @return this assertion
     */
    public ConditionAssert isResolvedRefsFalse(String reason, String expectedMessage) {
        return isResolvedRefsFalse(reason, (String actualMessage) -> message().describedAs(actualMessage).isEqualTo(expectedMessage));
    }

    /**
     * Asserts that this is a {@code ResolvedRefs} condition with status {@code False},
     * the given reason and a message satisfying the given requirements.
     *
     * @param reason the expected reason
     * @param messageAssertion the requirements the message must satisfy
     * @return this assertion
     */
    public ConditionAssert isResolvedRefsFalse(String reason, ThrowingConsumer<String> messageAssertion) {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(messageAssertion);
        return this;
    }

    /**
     * Asserts that this is a {@code ResolvedRefs} condition with status {@code True},
     * the standard reason and no message.
     *
     * @return this assertion
     */
    public ConditionAssert isResolvedRefsTrue() {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.ResolvedRefs.name());
        hasNoMessage();
        return this;
    }

    /**
     * Asserts that this is a {@code ResolvedRefs} condition with status {@code True},
     * the standard reason, no message, and an observed generation in sync with the given resource.
     *
     * @param thing the resource whose metadata generation the condition should reflect
     * @return this assertion
     */
    public ConditionAssert isResolvedRefsTrue(HasMetadata thing) {
        isResolvedRefsTrue()
                .hasObservedGenerationInSyncWithMetadataOf(thing);
        return this;
    }

    /**
     * Asserts that this is a {@code Ready} condition with status {@code Unknown}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param message the expected message
     * @return this assertion
     */
    public ConditionAssert isReadyUnknown(String reason, String message) {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    /**
     * Asserts that this is a {@code Ready} condition with status {@code False}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param message the expected message
     * @return this assertion
     */
    public ConditionAssert isReadyFalse(String reason, String message) {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    /**
     * Asserts that this is a {@code Ready} condition with status {@code True},
     * the standard reason and no message.
     *
     * @return this assertion
     */
    public ConditionAssert isReadyTrue() {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.Ready.name());
        hasNoMessage();
        return this;
    }

    /**
     * Asserts that this is an {@code Accepted} condition with status {@code Unknown}
     * and the given reason and message.
     *
     * @param reason the expected reason
     * @param message the expected message
     * @return this assertion
     */
    public ConditionAssert isAcceptedUnknown(String reason, String message) {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }
}
