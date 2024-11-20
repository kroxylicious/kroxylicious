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

import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;

public class ProxyConditionAssert extends AbstractObjectAssert<ProxyConditionAssert, Conditions> {
    protected ProxyConditionAssert(
                                   Conditions o) {
        super(o, ProxyConditionAssert.class);
    }

    public static ProxyConditionAssert assertThat(Conditions actual) {
        return new ProxyConditionAssert(actual);
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

    public ObjectAssert<Conditions.Status> status() {
        return Assertions.assertThat(actual.getStatus()).asInstanceOf(InstanceOfAssertFactories.type(Conditions.Status.class));
    }

    public AbstractZonedDateTimeAssert<?> lastTransitionTime() {
        return Assertions.assertThat(actual.getLastTransitionTime());
    }

    public ProxyConditionAssert isReady() {
        type().isEqualTo("Ready");
        status().isEqualTo(Conditions.Status.TRUE);
        reason().isEmpty();
        message().isEmpty();
        return this;
    }

    public ProxyConditionAssert hasObservedGeneration(long generation) {
        observedGeneration().isEqualTo(generation);
        return this;
    }

    public ProxyConditionAssert lastTransitionTimeIsAfter(ZonedDateTime time) {
        lastTransitionTime().isAfter(time);
        return this;
    }

    public ProxyConditionAssert lastTransitionTimeIsEqualTo(ZonedDateTime time) {
        lastTransitionTime().isEqualTo(time);
        return this;
    }
}
