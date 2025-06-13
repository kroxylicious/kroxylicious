/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxySpec;
import io.kroxylicious.kubernetes.api.v1alpha1.kafkaproxyspec.Infrastructure;

public class Labels {

    private Labels() {
        // singleton
    }

    public static Map<String, String> standardLabels(KafkaProxy proxy) {
        Map<String, String> labels = new LinkedHashMap<>();
        labels.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        labels.put("app.kubernetes.io/name", "kroxylicious");
        labels.put("app.kubernetes.io/component", "proxy");
        labels.put("app.kubernetes.io/instance", ResourcesUtil.name(proxy));
        return labels;
    }

    public static Map<String, String> infrastructureLabels(KafkaProxy proxy) {
        return Optional.ofNullable(proxy.getSpec())
                .map(KafkaProxySpec::getInfrastructure)
                .map(Infrastructure::getLabels)
                .orElse(Map.of());
    }

}
