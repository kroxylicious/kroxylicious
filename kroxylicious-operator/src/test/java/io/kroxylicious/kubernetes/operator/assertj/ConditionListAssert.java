/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.util.Lists;

import io.kroxylicious.kubernetes.api.common.Condition;

public class ConditionListAssert extends AbstractListAssert<ConditionListAssert, List<Condition>, Condition, ConditionAssert> {
    protected ConditionListAssert(
                                  List<Condition> o) {
        super(o, ConditionListAssert.class);
    }

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

    public ConditionListAssert containsOnlyTypes(Condition.Type... types) {
        Map<Condition.Type, Condition> s = actual.stream().collect(Collectors.toMap(Condition::getType, Function.identity()));
        Assertions.assertThat(s).as("unexpected types in list").containsOnlyKeys(types);
        return this;
    }

    public ConditionAssert singleOfType(Condition.Type type) {
        var ofType = actual.stream().filter(condition -> type.equals(condition.getType())).toList();
        assertThat(ofType).as("expected exactly one condition with type=" + type).hasSize(1);
        return ConditionAssert.assertThat(ofType.get(0)).as("type=" + type);
    }
}
