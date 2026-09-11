/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;

import io.kroxylicious.kubernetes.api.common.Condition;

/**
 * Assertions on a list of status {@link Condition}s.
 */
public class ConditionListAssert extends AbstractListAssert<ConditionListAssert, List<Condition>, Condition, ConditionAssert> {
    /**
     * Creates an assertion on the given condition list.
     *
     * @param o the condition list to assert on
     */
    protected ConditionListAssert(
                                  List<Condition> o) {
        super(o, ConditionListAssert.class);
    }

    /**
     * Creates an assertion on the given condition list.
     *
     * @param actual the condition list to assert on
     * @return a new assertion
     */
    public static ConditionListAssert assertThat(List<Condition> actual) {
        return new ConditionListAssert(actual);
    }

    @Override
    protected ConditionAssert toAssert(
                                       Condition value,
                                       String description) {
        return ConditionAssert.assertThat(value).as(description);
    }

    @Override
    protected ConditionListAssert newAbstractIterableAssert(Iterable<? extends Condition> iterable) {
        return assertThat(Lists.newArrayList(iterable));
    }

    /**
     * Asserts that the list contains conditions of exactly the given types and no others.
     *
     * @param types the expected condition types
     * @return this assertion
     */
    public ConditionListAssert containsOnlyTypes(Condition.Type... types) {
        Map<Condition.Type, Condition> s = actual.stream().collect(Collectors.toMap(Condition::getType, Function.identity()));
        Assertions.assertThat(s).as("unexpected types in list").containsOnlyKeys(types);
        return this;
    }

    /**
     * Asserts that the list contains exactly one condition of the given type and
     * returns an assertion on it.
     *
     * @param type the condition type to look for
     * @return an assertion on the single condition of the given type
     */
    public ConditionAssert singleOfType(Condition.Type type) {
        var ofType = actual.stream().filter(condition -> type.equals(condition.getType())).toList();
        assertThat(ofType).as("expected exactly one condition with type=" + type).hasSize(1);
        return ConditionAssert.assertThat(ofType.get(0)).as("type=" + type);
    }
}
