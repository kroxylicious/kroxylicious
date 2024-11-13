/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator.assertj;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyStatus;

public class StatusAssertions {
    public static StatusAssert assertThat(KafkaProxyStatus actual) {
        return StatusAssert.assertThat(actual);
    }
}
