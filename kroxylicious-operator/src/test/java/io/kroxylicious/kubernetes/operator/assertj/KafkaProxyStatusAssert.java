/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.api.ListAssert;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters;

public class KafkaProxyStatusAssert extends AbstractStatusAssert<KafkaProxyStatus, KafkaProxyStatusAssert> {
    protected KafkaProxyStatusAssert(
                                     KafkaProxyStatus o) {
        super(o, KafkaProxyStatusAssert.class,
                KafkaProxyStatus::getObservedGeneration,
                KafkaProxyStatus::getConditions);
    }

    public static KafkaProxyStatusAssert assertThat(KafkaProxyStatus actual) {
        return new KafkaProxyStatusAssert(actual);
    }

    public ListAssert<Clusters> clusters() {
        return Assertions.assertThat(actual.getClusters())
                .asInstanceOf(InstanceOfAssertFactories.list(io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxystatus.Clusters.class));
    }

    public ClusterAssert singleCluster() {
        return clusters().singleElement(AssertFactory.cluster());
    }
}
