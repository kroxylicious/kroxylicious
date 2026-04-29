/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import io.fabric8.kubernetes.client.KubernetesClientBuilder;

import static org.assertj.core.api.Assertions.assertThat;

class CreateKubernetesClientTest {

    @Test
    @SetEnvironmentVariable(key = "KUBERNETES_USER_AGENT", value = "kroxylicious-operator/0.12.0")
    void shouldUseUserAgentFromEnvironment() {
        try (var client = new KubernetesClientBuilder().build()) {
            assertThat(client.getConfiguration().getUserAgent())
                    .isEqualTo("kroxylicious-operator/0.12.0");
        }
    }
}
