/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.time.ZonedDateTime;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.AbstractZonedDateTimeAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ObjectAssert;

import io.kroxylicious.kubernetes.api.common.Condition;

public class ClusterConditionAssert extends AbstractObjectAssert<ClusterConditionAssert, Condition> {
    protected ClusterConditionAssert(
                                     Condition o) {
        super(o, ClusterConditionAssert.class);
    }

    public static ClusterConditionAssert assertThat(Condition actual) {
        return new ClusterConditionAssert(actual);
    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    public AbstractStringAssert<?> type() {
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

    public ClusterConditionAssert isReady() {
        type().isEqualTo("Ready");
        status().isEqualTo(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ClusterConditionAssert isAcceptedTrue() {
        type().isEqualTo("Accepted");
        status().isEqualTo(Condition.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ClusterConditionAssert isAcceptedFalse(String reason, String message) {
        type().isEqualTo("Accepted");
        status().isEqualTo(Condition.Status.FALSE);
        reason().isEqualTo(reason);
        message().isEqualTo(message);
        return this;
    }

    public ClusterConditionAssert hasObservedGeneration(long generation) {
        observedGeneration().isEqualTo(generation);
        return this;
    }

    public ClusterConditionAssert lastTransitionTimeIsAfter(ZonedDateTime time) {
        lastTransitionTime().isAfter(time);
        return this;
    }

    public ClusterConditionAssert lastTransitionTimeIsEqualTo(ZonedDateTime time) {
        lastTransitionTime().isEqualTo(time);
        return this;
    }
}
