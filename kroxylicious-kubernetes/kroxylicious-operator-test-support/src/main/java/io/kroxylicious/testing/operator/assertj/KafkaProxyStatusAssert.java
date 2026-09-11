/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

/**
 * Assertions on a {@link KafkaProxyStatus}.
 */
public class KafkaProxyStatusAssert extends AbstractStatusAssert<KafkaProxyStatus, KafkaProxyStatusAssert> {
    /**
     * Creates an assertion on the given status.
     *
     * @param o the status to assert on
     */
    protected KafkaProxyStatusAssert(
                                     KafkaProxyStatus o) {
        super(o, KafkaProxyStatusAssert.class,
                KafkaProxyStatus::getObservedGeneration,
                KafkaProxyStatus::getConditions);
    }

    /**
     * Creates an assertion on the given status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaProxyStatusAssert assertThat(KafkaProxyStatus actual) {
        return new KafkaProxyStatusAssert(actual);
    }

    @Override
    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    @Override
    public ListAssert<Condition.Status> conditions() {
        return Assertions.assertThat(actual.getConditions())
                .asInstanceOf(InstanceOfAssertFactories.list(Condition.Status.class));
    }

    @Override
    public ConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.condition());
    }

    /**
     * Returns a list assertion on the status's clusters.
     *
     * @return a list assertion on the clusters
     */
    public ListAssert<Clusters> clusters() {
        return Assertions.assertThat(actual.getClusters())
                .asInstanceOf(InstanceOfAssertFactories.list(io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters.class));
    }

    /**
     * Asserts that the status has the given replica count.
     *
     * @param expectedReplicaCount the expected replica count
     */
    public void replicas(int expectedReplicaCount) {
        Assertions.assertThat(actual.getReplicas()).isEqualTo(expectedReplicaCount);
    }
}
