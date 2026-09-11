/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.LinkedHashMap;
import java.util.Map;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

/**
 * Provides standard Kubernetes labels for resources managed by the operator.
 */
public class Labels {

    private Labels() {
        // singleton
    }

    /**
     * Returns the standard Kubernetes labels to apply to resources owned by the given proxy.
     * @param proxy the KafkaProxy resource
     * @return a map of standard label key-value pairs
     */
    public static Map<String, String> standardLabels(KafkaProxy proxy) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        labels.put("app.kubernetes.io/name", "kroxylicious");
        labels.put("app.kubernetes.io/component", "proxy");
        labels.put("app.kubernetes.io/instance", ResourcesUtil.name(proxy));
        return labels;
    }

}
