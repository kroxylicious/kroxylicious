/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.VirtualKafkaClusterStatus;

/**
 * Assertions on a {@link VirtualKafkaClusterStatus}.
 */
public class VirtualKafkaClusterStatusAssert extends AbstractStatusAssert<VirtualKafkaClusterStatus, VirtualKafkaClusterStatusAssert> {
    /**
     * Creates an assertion on the given status.
     *
     * @param o the status to assert on
     */
    protected VirtualKafkaClusterStatusAssert(
                                              VirtualKafkaClusterStatus o) {
        super(o, VirtualKafkaClusterStatusAssert.class,
                VirtualKafkaClusterStatus::getObservedGeneration,
                VirtualKafkaClusterStatus::getConditions);
    }

    /**
     * Creates an assertion on the given status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static VirtualKafkaClusterStatusAssert assertThat(VirtualKafkaClusterStatus actual) {
        return new VirtualKafkaClusterStatusAssert(actual);
    }

}
