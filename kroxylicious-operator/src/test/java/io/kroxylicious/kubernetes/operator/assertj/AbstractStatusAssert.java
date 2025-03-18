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

public abstract class AbstractStatusAssert<STATUS, SELF extends AbstractStatusAssert<STATUS, SELF>> extends AbstractObjectAssert<SELF, STATUS> {
    private final Function<STATUS, Long> observedGenerationAccessor;
    private final Function<STATUS, List<Condition>> conditionsAccessor;

    public AbstractStatusAssert(
                                STATUS actual,
                                Class<SELF> selfType,
                                Function<STATUS, Long> observedGenerationAccessor,
                                Function<STATUS, List<Condition>> conditionsAccessor) {
        super(actual, selfType);
        this.observedGenerationAccessor = observedGenerationAccessor;
        this.conditionsAccessor = conditionsAccessor;

    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(observedGenerationAccessor.apply(actual));
    }

    public SELF hasObservedGenerationInSyncWithMetadataOf(HasMetadata thing) {
        observedGeneration().isEqualTo(thing.getMetadata().getGeneration());
        return (SELF) this;
    }

    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(conditionsAccessor.apply(actual))
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }
}
