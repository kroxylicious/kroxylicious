/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import org.assertj.core.api.InstanceOfAssertFactory;

import io.kroxylicious.kubernetes.proxy.api.v1alpha1.ProxyStatus;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.Clusters;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.Conditions;

public class AssertFactory {
    public static InstanceOfAssertFactory<ProxyStatus, StatusAssert> status() {
        return new InstanceOfAssertFactory<>(ProxyStatus.class, StatusAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Conditions, ProxyConditionAssert> proxyCondition() {
        return new InstanceOfAssertFactory<>(Conditions.class, ProxyConditionAssert::assertThat);
    }

    public static InstanceOfAssertFactory<Clusters, ClusterAssert> cluster() {
        return new InstanceOfAssertFactory<>(Clusters.class, ClusterAssert::assertThat);
    }

    public static InstanceOfAssertFactory<io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.clusters.Conditions, ClusterConditionAssert> clusterCondition() {
        return new InstanceOfAssertFactory<>(io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.clusters.Conditions.class, ClusterConditionAssert::assertThat);
    }
}
