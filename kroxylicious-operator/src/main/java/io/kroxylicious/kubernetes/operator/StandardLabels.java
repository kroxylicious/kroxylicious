/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.fabric8.kubernetes.api.model.ObjectMetaFluent;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

public class StandardLabels {

    static <T extends ObjectMetaFluent<T>> T addStandardLabels(KafkaProxy proxy, T fluent) {
        return fluent.addToLabels("app.kubernetes.io/part-of", proxy.getMetadata().getName())
                .addToLabels("app.kubernetes.io/managed-by", "kroxylicious-operator");
    }
}
