/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.clusters.Conditions;

public class OperatorAssertions {
    public static StatusAssert assertThat(KafkaProxyStatus actual) {
        return StatusAssert.assertThat(actual);
    }

    public static ClusterAssert assertThat(Clusters actual) {
        return ClusterAssert.assertThat(actual);
    }

    public static ClusterConditionAssert assertThat(Conditions actual) {
        return ClusterConditionAssert.assertThat(actual);
    }

    public static ProxyConditionAssert assertThat(io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Conditions actual) {
        return ProxyConditionAssert.assertThat(actual);
    }
}
