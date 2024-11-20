/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.InstanceOfAssertFactory;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions;

public class AssertFactory {
    public static InstanceOfAssertFactory<KafkaProxyStatus, StatusAssert> status() {
        return new InstanceOfAssertFactory<>(KafkaProxyStatus.class, StatusAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Conditions, ProxyConditionAssert> proxyCondition() {
        return new InstanceOfAssertFactory<>(Conditions.class, ProxyConditionAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Clusters, ClusterAssert> cluster() {
        return new InstanceOfAssertFactory<>(Clusters.class, ClusterAssert::assertThat);
    }

    public static InstanceOfAssertFactory<io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions, ClusterConditionAssert> clusterCondition() {
        return new InstanceOfAssertFactory<>(io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions.class, ClusterConditionAssert::assertThat);
    }
}
