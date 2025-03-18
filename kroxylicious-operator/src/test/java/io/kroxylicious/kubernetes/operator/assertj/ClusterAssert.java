/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

public class ClusterAssert extends AbstractObjectAssert<ClusterAssert, Clusters> {
    protected ClusterAssert(
                            Clusters o) {
        super(o, ClusterAssert.class);
    }

    public static ClusterAssert assertThat(Clusters actual) {
        return new ClusterAssert(actual);
    }

    public AbstractStringAssert<?> name() {
        return Assertions.assertThat(actual.getName());
    }

    public ClusterAssert nameIsEqualTo(String s) {
        name().isEqualTo(s);
        return this;
    }

    public ListAssert<Condition> conditions() {
        return Assertions.assertThat(actual.getConditions()).asInstanceOf(InstanceOfAssertFactories.list(Condition.class));
    }

    public ClusterConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.clusterCondition());
    }

}
