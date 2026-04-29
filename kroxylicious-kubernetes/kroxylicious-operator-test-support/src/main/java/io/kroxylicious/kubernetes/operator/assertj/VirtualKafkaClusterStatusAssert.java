/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;

public class VirtualKafkaClusterStatusAssert extends AbstractStatusAssert<VirtualKafkaClusterStatus, VirtualKafkaClusterStatusAssert> {
    protected VirtualKafkaClusterStatusAssert(
                                              VirtualKafkaClusterStatus o) {
        super(o, VirtualKafkaClusterStatusAssert.class,
                VirtualKafkaClusterStatus::getObservedGeneration,
                VirtualKafkaClusterStatus::getConditions);
    }

    public static VirtualKafkaClusterStatusAssert assertThat(VirtualKafkaClusterStatus actual) {
        return new VirtualKafkaClusterStatusAssert(actual);
    }

}
