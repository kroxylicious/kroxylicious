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

    public AbstractComparableAssert<?, Condition.Type> type() {
        return Assertions.assertThat(actual.getType());
    }

    public AbstractStringAssert<?> message() {
        return Assertions.assertThat(actual.getMessage());
    }

    public AbstractStringAssert<?> reason() {
        return Assertions.assertThat(actual.getReason());
    }

    public ObjectAssert<Condition.Status> status() {
        return Assertions.assertThat(actual.getStatus()).asInstanceOf(InstanceOfAssertFactories.type(Condition.Status.class));
    }

    public AbstractZonedDateTimeAssert<?> lastTransitionTime() {
        return Assertions.assertThat(actual.getLastTransitionTime());
    }

    public ConditionAssert isReady() {
        type().isEqualTo(Condition.Type.Ready);
        status().isEqualTo(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ConditionAssert isAcceptedTrue() {
        type().isEqualTo(Condition.Type.Accepted);
        status().isEqualTo(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ConditionAssert isAcceptedFalse(String reason, String message) {
        type().isEqualTo(Condition.Type.Accepted);
        status().isEqualTo(Condition.Status.FALSE);
        reason().isEqualTo(reason);
        message().isEqualTo(message);
        return this;
    }

    public ConditionAssert isResolvedRefsFalse(String reason, String message) {
        type().isEqualTo(Condition.Type.ResolvedRefs);
        status().isEqualTo(Condition.Status.FALSE);
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
