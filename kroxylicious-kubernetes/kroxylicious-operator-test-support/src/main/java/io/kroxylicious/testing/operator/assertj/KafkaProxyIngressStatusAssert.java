/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;

/**
 * Assertions on a {@link KafkaProxyIngressStatus}.
 */
public class KafkaProxyIngressStatusAssert extends AbstractStatusAssert<KafkaProxyIngressStatus, KafkaProxyIngressStatusAssert> {
    /**
     * Creates an assertion on the given status.
     *
     * @param o the status to assert on
     */
    protected KafkaProxyIngressStatusAssert(
                                            KafkaProxyIngressStatus o) {
        super(o, KafkaProxyIngressStatusAssert.class,
                KafkaProxyIngressStatus::getObservedGeneration,
                KafkaProxyIngressStatus::getConditions);
    }

    /**
     * Creates an assertion on the given status.
     *
     * @param actual the status to assert on
     * @return a new assertion
     */
    public static KafkaProxyIngressStatusAssert assertThat(KafkaProxyIngressStatus actual) {
        return new KafkaProxyIngressStatusAssert(actual);
    }

}
