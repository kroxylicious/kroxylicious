/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.util.List;
import java.util.function.Function;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.fabric8.kubernetes.api.model.HasMetadata;

import io.kroxylicious.kubernetes.api.common.Condition;

/**
 * Base class for assertions on custom resource statuses, providing assertions on the
 * {@code observedGeneration} and {@code conditions} common to all Kroxylicious statuses.
 *
 * @param <A> the status type
 * @param <S> the self type
 */
@SuppressWarnings("java:S2160") // equals on superclass checks reference equality, not object equality
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

    /**
     * Returns an assertion on the status's observed generation.
     *
     * @return an assertion on the observed generation
     */
    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(observedGenerationAccessor.apply(actual));
    }

    /**
     * Asserts that the status has the given observed generation.
     *
     * @param observedGeneration the expected observed generation
     * @return this assertion
     */
    @SuppressWarnings("unchecked")
    public S hasObservedGeneration(Long observedGeneration) {
        observedGeneration().isEqualTo(observedGeneration);
        return (S) this;
    }

    /**
     * Asserts that the status's observed generation equals the metadata generation of the given resource.
     *
     * @param thing the resource whose metadata generation the status should reflect
     * @return this assertion
     */
    @SuppressWarnings("unchecked")
    public S hasObservedGenerationInSyncWithMetadataOf(HasMetadata thing) {
        hasObservedGeneration(thing.getMetadata().getGeneration());
        return (S) this;
    }

    /**
     * Returns a list assertion on the status's conditions.
     *
     * @return a list assertion on the conditions
     */
    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(conditionsAccessor.apply(actual))
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    /**
     * Asserts that the status has exactly one condition and returns an assertion on it.
     *
     * @return an assertion on the single condition
     */
    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

    /**
     * Returns an assertion on the status's condition list.
     *
     * @return an assertion on the condition list
     */
    public ConditionListAssert conditionList() {
        var conditions = conditionsAccessor.apply(actual);
        return ConditionListAssert.assertThat(conditions);
    }

    /**
     * Returns an assertion on the status's first condition.
     *
     * @return an assertion on the first condition
     */
    public ConditionAssert firstCondition() {
        return conditions().first(AssertFactory.condition());
    }

    /**
     * Returns an assertion on the status's last condition.
     *
     * @return an assertion on the last condition
     */
    public ConditionAssert lastCondition() {
        return conditions().last(AssertFactory.condition());
    }
}
