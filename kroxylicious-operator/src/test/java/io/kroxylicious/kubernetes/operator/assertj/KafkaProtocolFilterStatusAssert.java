/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.filter.api.v1alpha1.KafkaProtocolFilterStatus;

public class KafkaProtocolFilterStatusAssert extends AbstractStatusAssert<KafkaProtocolFilterStatus, KafkaProtocolFilterStatusAssert> {
    protected KafkaProtocolFilterStatusAssert(
                                              KafkaProtocolFilterStatus o) {
        super(o, KafkaProtocolFilterStatusAssert.class,
                KafkaProtocolFilterStatus::getObservedGeneration,
                KafkaProtocolFilterStatus::getConditions);
    }

    public static KafkaProtocolFilterStatusAssert assertThat(KafkaProtocolFilterStatus actual) {
        return new KafkaProtocolFilterStatusAssert(actual);
    }
}