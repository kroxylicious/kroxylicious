/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProtocolFilterStatus;

/**
 * Assertions on a {@link KafkaProtocolFilterStatus}.
 */
public class KafkaProtocolFilterStatusAssert extends AbstractStatusAssert<KafkaProtocolFilterStatus, KafkaProtocolFilterStatusAssert> {
    /**
     * Creates an assertion on the given status.
     *
     * @param o the status to assert on
     */
    protected KafkaProtocolFilterStatusAssert(
                                              KafkaProtocolFilterStatus o) {
        super(o, KafkaProtocolFilterStatusAssert.class,
                KafkaProtocolFilterStatus::getObservedGeneration,
                KafkaProtocolFilterStatus::getConditions);
    }

    /**
     * Creates an assertion on the given status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaProtocolFilterStatusAssert assertThat(KafkaProtocolFilterStatus actual) {
        return new KafkaProtocolFilterStatusAssert(actual);
    }
}