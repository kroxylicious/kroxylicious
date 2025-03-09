/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class LabelsTest {

    public static final String PROXY_NAME = "kproxy";

    // labels don't technically need to be ordered, but deterministic output reduces noise when comparing output YAML
    @Test
    void standardLabelsAreDeterministicallyOrdered() {
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata().build();
        Map<String, String> labels = Labels.standardLabels(proxy);
        LinkedHashMap<String, String> expected = new LinkedHashMap<>();
        expected.put("app.kubernetes.io/part-of", "kafka");
        expected.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        expected.put("app.kubernetes.io/name", "kroxylicious-proxy");
        expected.put("app.kubernetes.io/instance", PROXY_NAME);
        expected.put("app.kubernetes.io/component", "proxy");
        assertThat(labels).containsExactlyEntriesOf(expected);
    }

}
