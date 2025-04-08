/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.fabric8.kubernetes.api.model.PodTemplateSpec;
import io.fabric8.kubernetes.api.model.PodTemplateSpecBuilder;

import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxy;
import io.kroxylicious.kubernetes.api.v1alpha1.KafkaProxyBuilder;

import static io.kroxylicious.kubernetes.operator.ProxyDeploymentDependentResource.KROXYLICIOUS_IMAGE_ENV_VAR;
import static org.assertj.core.api.Assertions.assertThat;

class ProxyDeploymentTest {

    public static final String PROXY_NAME = "kproxy";

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR)
    void operandImageDefault() {
        assertThat(ProxyDeploymentDependentResource.getOperandImage())
                .matches("^quay.io/kroxylicious/kroxylicious:.*");
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR, value = "quay.io/myorg/kroxylicious:1")
    void operandImageOverrideFromEnvironment() {
        assertThat(ProxyDeploymentDependentResource.getOperandImage())
                .isEqualTo("quay.io/myorg/kroxylicious:1");
    }

    // labels don't technically need to be ordered, but deterministic output reduces noise when comparing output YAML
    @Test
    void podLabelsDeterministicallyOrdered() {
        PodTemplateSpec podTemplate = new PodTemplateSpecBuilder().withNewMetadata().addToLabels("c", "d").addToLabels("a", "b").endMetadata().build();
        KafkaProxy proxy = new KafkaProxyBuilder().withNewMetadata().withName(PROXY_NAME).endMetadata()
                .withNewSpec().withPodTemplate(podTemplate).endSpec().build();
        Map<String, String> labels = ProxyDeploymentDependentResource.podLabels(proxy);
        LinkedHashMap<String, String> expected = new LinkedHashMap<>();
        expected.put("app", "kroxylicious");
        expected.put("c", "d");
        expected.put("a", "b");
        expected.put("app.kubernetes.io/part-of", "kafka");
        expected.put("app.kubernetes.io/managed-by", "kroxylicious-operator");
        expected.put("app.kubernetes.io/name", "kroxylicious-proxy");
        expected.put("app.kubernetes.io/instance", PROXY_NAME);
        expected.put("app.kubernetes.io/component", "proxy");
        assertThat(labels).containsExactlyEntriesOf(expected);
    }
}
