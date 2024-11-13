/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.HashMap;
import java.util.Map;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;

public class Labels {

    public static Map<String, String> standardLabels(KafkaProxy proxy) {
        HashMap<String, String> labels = new HashMap<>();
        labels.put("app.kubernetes.io/part-of", "kafka");
        labels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        labels.put("app.kubernetes.io/name", "kroxylicious-proxy");
        labels.put("app.kubernetes.io/instance", proxy.getMetadata().getName());
        labels.put("app.kubernetes.io/component", "proxy");
        return labels;
    }

}
