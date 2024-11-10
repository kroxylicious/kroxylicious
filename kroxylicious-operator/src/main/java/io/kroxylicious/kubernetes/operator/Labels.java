/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

import java.util.Map;

public class Labels {

    private static final String KROXYLICIOUS_OPERATOR = "kroxylicious-operator";
    private static final String MANAGED_BY_LABEL = "app.kubernetes.io/managed-by";
    public static final String MANAGED_BY_SELECTOR = MANAGED_BY_LABEL + "=" + KROXYLICIOUS_OPERATOR;

    public static Map<String, String> standardLabels(KafkaProxy proxy) {
        return Map.of("app.kubernetes.io/part-of", proxy.getMetadata().getName(), MANAGED_BY_LABEL, KROXYLICIOUS_OPERATOR);
    }

}
