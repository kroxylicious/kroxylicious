/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

/**
 * Assertions on a cluster entry ({@link Clusters}) within a {@code KafkaProxy} status.
 */
public class ClusterAssert extends AbstractObjectAssert<ClusterAssert, Clusters> {
    /**
     * Creates an assertion on the given cluster.
     *
     * @param o the cluster to assert on
     */
    protected ClusterAssert(
                            Clusters o) {
        super(o, ClusterAssert.class);
    }

    /**
     * Creates an assertion on the given cluster.
     *
     * @param actual the cluster to assert on
     * @return a new assertion
     */
    public static ClusterAssert assertThat(Clusters actual) {
        return new ClusterAssert(actual);
    }

    /**
     * Returns an assertion on the cluster's name.
     *
     * @return an assertion on the cluster's name
     */
    public AbstractStringAssert<?> name() {
        return Assertions.assertThat(actual.getName());
    }

    /**
     * Asserts that the cluster has the given name.
     *
     * @param s the expected name
     * @return this assertion
     */
    public ClusterAssert nameIsEqualTo(String s) {
        name().isEqualTo(s);
        return this;
    }

    /**
     * Returns a list assertion on the cluster's conditions.
     *
     * @return a list assertion on the conditions
     */
    public ListAssert<Condition> conditions() {
        return Assertions.assertThat(actual.getConditions()).asInstanceOf(InstanceOfAssertFactories.list(Condition.class));
    }

    /**
     * Asserts that the cluster has exactly one condition and returns an assertion on it.
     *
     * @return an assertion on the single condition
     */
    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

}
