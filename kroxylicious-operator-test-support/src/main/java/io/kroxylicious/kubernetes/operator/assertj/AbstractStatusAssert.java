/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.List;
import java.util.function.Function;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;

abstract class AbstractStatusAssert<A, S extends AbstractStatusAssert<A, S>> extends AbstractObjectAssert<S, A> {
    private final Function<A, Long> observedGenerationAccessor;
    private final Function<A, List<Condition>> conditionsAccessor;

    AbstractStatusAssert(
            A actual,
            Class<S> selfType,
            Function<A, Long> observedGenerationAccessor,
            Function<A, List<Condition>> conditionsAccessor) {
        super(actual, selfType);
        this.observedGenerationAccessor = observedGenerationAccessor;
        this.conditionsAccessor = conditionsAccessor;

    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(observedGenerationAccessor.apply(actual));
    }

    @SuppressWarnings("unchecked")
    public S hasObservedGeneration(Long observedGeneration) {
        observedGeneration().isEqualTo(observedGeneration);
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    public S hasObservedGenerationInSyncWithMetadataOf(HasMetadata thing) {
        hasObservedGeneration(thing.getMetadata().getGeneration());
        return (S) this;
    }

    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(conditionsAccessor.apply(actual))
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

    public ConditionListAssert conditionList() {
        var conditions = conditionsAccessor.apply(actual);
        return ConditionListAssert.assertThat(conditions);
    }

    public ConditionAssert firstCondition() {
        return conditions().first(AssertFactory.condition());
    }

    public ConditionAssert lastCondition() {
        return conditions().last(AssertFactory.condition());
    }
}
