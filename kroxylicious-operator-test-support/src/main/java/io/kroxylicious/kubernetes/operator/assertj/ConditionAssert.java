/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

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

@SuppressWarnings("UnusedReturnValue")
public class ConditionAssert extends AbstractObjectAssert<ConditionAssert, Condition> {
    protected ConditionAssert(
                              Condition o) {
        super(o, ConditionAssert.class);
    }

    public static ConditionAssert assertThat(Condition actual) {
        return new ConditionAssert(actual);
    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    public ConditionAssert hasObservedGeneration(Long expected) {
        observedGeneration().isEqualTo(expected);
        return this;
    }

    public ConditionAssert hasObservedGenerationInSyncWithMetadataOf(HasMetadata thing) {
        hasObservedGeneration(thing.getMetadata().getGeneration());
        return this;
    }

    public AbstractComparableAssert<?, Condition.Type> type() {
        return Assertions.assertThat(actual.getType());
    }

    public ConditionAssert hasType(Condition.Type expected) {
        type().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    public AbstractStringAssert<?> message() {
        return Assertions.assertThat(actual.getMessage());
    }

    public ConditionAssert hasNoMessage() {
        message().isEmpty();
        return this;
    }

    public ConditionAssert hasMessage(String expected) {
        message().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    public ConditionAssert hasMessage(ThrowingConsumer<String> expected) {
        message().describedAs(actual.toString()).satisfies(expected);
        return this;
    }

    public AbstractStringAssert<?> reason() {
        return Assertions.assertThat(actual.getReason());
    }

    public ConditionAssert hasReason(String expected) {
        reason().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    public ObjectAssert<Condition.Status> status() {
        return Assertions.assertThat(actual.getStatus()).asInstanceOf(InstanceOfAssertFactories.type(Condition.Status.class));
    }

    public ConditionAssert hasStatus(Condition.Status expected) {
        status().describedAs(actual.toString()).isEqualTo(expected);
        return this;
    }

    public AbstractInstantAssert<?> lastTransitionTime() {
        return Assertions.assertThat(actual.getLastTransitionTime());
    }

    public ConditionAssert hasLastTransitionTime(Instant time) {
        lastTransitionTime().isEqualTo(time);
        return this;
    }

    public ConditionAssert isAcceptedTrue() {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.Accepted.name());
        hasNoMessage();
        return this;
    }

    public ConditionAssert isAcceptedTrue(HasMetadata thing) {
        isAcceptedTrue()
                .hasObservedGenerationInSyncWithMetadataOf(thing);
        return this;
    }

    public ConditionAssert isAcceptedFalse(String reason, String message) {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    public ConditionAssert isResolvedRefsUnknown(String reason, String message) {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    public ConditionAssert isResolvedRefsFalse(String reason, String expectedMessage) {
        return isResolvedRefsFalse(reason, (String actualMessage) -> message().describedAs(actualMessage).isEqualTo(expectedMessage));
    }

    public ConditionAssert isResolvedRefsFalse(String reason, ThrowingConsumer<String> messageAssertion) {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(messageAssertion);
        return this;
    }

    public ConditionAssert isResolvedRefsTrue() {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.ResolvedRefs.name());
        hasNoMessage();
        return this;
    }

    public ConditionAssert isResolvedRefsTrue(HasMetadata thing) {
        isResolvedRefsTrue()
                .hasObservedGenerationInSyncWithMetadataOf(thing);
        return this;
    }

    public ConditionAssert isReadyUnknown(String reason, String message) {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    public ConditionAssert isReadyFalse(String reason, String message) {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.FALSE);
        hasReason(reason);
        hasMessage(message);
        return this;
    }

    public ConditionAssert isReadyTrue() {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.TRUE);
        hasReason(Condition.Type.Ready.name());
        hasNoMessage();
        return this;
    }

    public ConditionAssert isAcceptedUnknown(String reason, String message) {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.UNKNOWN);
        hasReason(reason);
        hasMessage(message);
        return this;
    }
}
