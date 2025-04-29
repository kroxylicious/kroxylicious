/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaServiceStatus;

public class KafkaServiceStatusAssert extends AbstractStatusAssert<KafkaServiceStatus, KafkaServiceStatusAssert> {
    protected KafkaServiceStatusAssert(
                                       KafkaServiceStatus o) {
        super(o, KafkaServiceStatusAssert.class,
                KafkaServiceStatus::getObservedGeneration,
                KafkaServiceStatus::getConditions);
    }

    public static KafkaServiceStatusAssert assertThat(KafkaServiceStatus actual) {
        return new KafkaServiceStatusAssert(actual);
    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(actual.getConditions())
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

}
