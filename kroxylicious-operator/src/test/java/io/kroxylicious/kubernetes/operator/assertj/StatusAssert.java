/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.AbstractLongAssert;
import org.assertj.core.api.AbstractObjectAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

public class StatusAssert extends AbstractObjectAssert<StatusAssert, KafkaProxyStatus> {
    protected StatusAssert(
                           KafkaProxyStatus o) {
        super(o, StatusAssert.class);
    }

    public static StatusAssert assertThat(KafkaProxyStatus actual) {
        return new StatusAssert(actual);
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

    public ListAssert<Clusters> clusters() {
        return Assertions.assertThat(actual.getClusters())
                .asInstanceOf(InstanceOfAssertFactories.list(io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters.class));
    }

    public ClusterAssert singleCluster() {
        return clusters().singleElement(AssertFactory.cluster());
    }
}
