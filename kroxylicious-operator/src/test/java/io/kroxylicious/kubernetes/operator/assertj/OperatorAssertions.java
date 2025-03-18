/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.api.common.Condition;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

public class OperatorAssertions {
    public static KafkaProxyStatusAssert assertThat(KafkaProxyStatus actual) {
        return KafkaProxyStatusAssert.assertThat(actual);
    }

    public static ClusterAssert assertThat(Clusters actual) {
        return ClusterAssert.assertThat(actual);
    }

    public static ConditionAssert assertThat(Condition actual) {
        return ConditionAssert.assertThat(actual);
    }
}
