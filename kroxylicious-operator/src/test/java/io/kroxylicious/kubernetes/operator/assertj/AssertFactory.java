/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.InstanceOfAssertFactory;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

public class AssertFactory {
    public static InstanceOfAssertFactory<KafkaProxyStatus, StatusAssert> status() {
        return new InstanceOfAssertFactory<>(KafkaProxyStatus.class, StatusAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Condition, ClusterConditionAssert> proxyCondition() {
        return new InstanceOfAssertFactory<>(Condition.class, ClusterConditionAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Clusters, ClusterAssert> cluster() {
        return new InstanceOfAssertFactory<>(Clusters.class, ClusterAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Condition, ClusterConditionAssert> clusterCondition() {
        return new InstanceOfAssertFactory<>(Condition.class, ClusterConditionAssert::assertThat);
    }
}
