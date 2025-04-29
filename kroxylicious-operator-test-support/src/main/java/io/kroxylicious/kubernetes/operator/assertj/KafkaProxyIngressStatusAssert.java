/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyIngressStatus;

public class KafkaProxyIngressStatusAssert extends AbstractStatusAssert<KafkaProxyIngressStatus, KafkaProxyIngressStatusAssert> {
    protected KafkaProxyIngressStatusAssert(
                                            KafkaProxyIngressStatus o) {
        super(o, KafkaProxyIngressStatusAssert.class,
                KafkaProxyIngressStatus::getObservedGeneration,
                KafkaProxyIngressStatus::getConditions);
    }

    public static KafkaProxyIngressStatusAssert assertThat(KafkaProxyIngressStatus actual) {
        return new KafkaProxyIngressStatusAssert(actual);
    }

}
