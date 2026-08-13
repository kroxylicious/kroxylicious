/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import org.assertj.core.api.InstanceOfAssertFactory;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

/**
 * Factories for use with AssertJ's {@code asInstanceOf} and element navigation methods,
 * producing the custom assertions in this package.
 */
public class AssertFactory {
    /**
     * Prevent construction of utility class
     */
    private AssertFactory() {
        // private constructor
    }

    /**
     * Returns a factory producing {@link KafkaProxyStatusAssert}.
     *
     * @return a factory producing {@link KafkaProxyStatusAssert}
     */
    public static InstanceOfAssertFactory<KafkaProxyStatus, KafkaProxyStatusAssert> status() {
        return new InstanceOfAssertFactory<>(KafkaProxyStatus.class, KafkaProxyStatusAssert::assertThat);
    }

    /**
     * Returns a factory producing {@link ConditionAssert}.
     *
     * @return a factory producing {@link ConditionAssert}
     */
    public static InstanceOfAssertFactory<Condition, ConditionAssert> condition() {
        return new InstanceOfAssertFactory<>(Condition.class, ConditionAssert::assertThat);
    }

    /**
     * Returns a factory producing {@link ClusterAssert}.
     *
     * @return a factory producing {@link ClusterAssert}
     */
    public static InstanceOfAssertFactory<Clusters, ClusterAssert> cluster() {
        return new InstanceOfAssertFactory<>(Clusters.class, ClusterAssert::assertThat);
    }

}
