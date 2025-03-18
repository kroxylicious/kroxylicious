/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.time.ZonedDateTime;

import org.assertj.core.api.AbstractComparableAssert;
import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.AbstractZonedDateTimeAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;

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
        observedGeneration().isEqualTo(thing.getMetadata().getGeneration());
        return this;
    }

    public AbstractComparableAssert<?, Condition.Type> type() {
        return Assertions.assertThat(actual.getType());
    }

    public ConditionAssert hasType(Condition.Type expected) {
        AbstractComparableAssert<?, Condition.Type> equalTo = type().isEqualTo(expected);
        return this;
    }

    public AbstractStringAssert<?> message() {
        return Assertions.assertThat(actual.getMessage());
    }

    public ConditionAssert hasNoMessage() {
        message().isNull();
        return this;
    }

    public ConditionAssert hasMessage(String expected) {
        message().isEqualTo(expected);
        return this;
    }

    public AbstractStringAssert<?> reason() {
        return Assertions.assertThat(actual.getReason());
    }

    public ConditionAssert hasNoReason() {
        reason().isNull();
        return this;
    }

    public ConditionAssert hasReason(String expected) {
        reason().isEqualTo(expected);
        return this;
    }

    public ObjectAssert<Condition.Status> status() {
        return Assertions.assertThat(actual.getStatus()).asInstanceOf(InstanceOfAssertFactories.type(Condition.Status.class));
    }

    public ConditionAssert hasStatus(Condition.Status expected) {
        status().isEqualTo(expected);
        return this;
    }

    public AbstractZonedDateTimeAssert<?> lastTransitionTime() {
        return Assertions.assertThat(actual.getLastTransitionTime());
    }

    public ConditionAssert isReady() {
        hasType(Condition.Type.Ready);
        hasStatus(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ConditionAssert isAcceptedTrue() {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ConditionAssert isAcceptedFalse(String reason, String message) {
        hasType(Condition.Type.Accepted);
        hasStatus(Condition.Status.FALSE);
        reason().isEqualTo(reason);
        message().isEqualTo(message);
        return this;
    }

    public ConditionAssert isResolvedRefsFalse(String reason, String message) {
        hasType(Condition.Type.ResolvedRefs);
        hasStatus(Condition.Status.FALSE);
        reason().isEqualTo(reason);
        message().isEqualTo(message);
        return this;
    }

    public ConditionAssert hasObservedGeneration(long generation) {
        observedGeneration().isEqualTo(generation);
        return this;
    }

    public ConditionAssert lastTransitionTimeIsAfter(ZonedDateTime time) {
        lastTransitionTime().isAfter(time);
        return this;
    }

    public ConditionAssert lastTransitionTimeIsEqualTo(ZonedDateTime time) {
        lastTransitionTime().isEqualTo(time);
        return this;
    }

}
