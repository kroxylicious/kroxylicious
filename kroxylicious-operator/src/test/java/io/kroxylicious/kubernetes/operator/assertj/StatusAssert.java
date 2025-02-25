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

import io.kroxylicious.kubernetes.proxy.api.v1alpha1.ProxyStatus;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.Clusters;
import io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.Conditions;

public class StatusAssert extends AbstractObjectAssert<StatusAssert, ProxyStatus> {
    protected StatusAssert(
                           ProxyStatus o) {
        super(o, StatusAssert.class);
    }

    public static StatusAssert assertThat(ProxyStatus actual) {
        return new StatusAssert(actual);
    }

    public AbstractLongAssert<?> observedGeneration() {
        return Assertions.assertThat(actual.getObservedGeneration());
    }

    public ListAssert<Conditions.Status> conditions() {
        return Assertions.assertThat(actual.getConditions())
                .asInstanceOf(InstanceOfAssertFactories.list(Conditions.Status.class));
    }

    public ProxyConditionAssert singleCondition() {
        return conditions().singleElement(AssertFactory.proxyCondition());
    }

    public ListAssert<Clusters> clusters() {
        return Assertions.assertThat(actual.getClusters())
                .asInstanceOf(InstanceOfAssertFactories.list(io.kroxylicious.kubernetes.proxy.api.v1alpha1.proxystatus.Clusters.class));
    }

    public ClusterAssert singleCluster() {
        return clusters().singleElement(AssertFactory.cluster());
    }
}
