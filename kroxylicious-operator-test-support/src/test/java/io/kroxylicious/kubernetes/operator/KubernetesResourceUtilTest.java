/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.ConfigMapBuilder;

import static io.kroxylicious.kubernetes.operator.KubernetesResourceUtil.name;
import static org.assertj.core.api.Assertions.assertThat;

class KubernetesResourceUtilTest {

    @Test
    void shouldExtractName() {
        ConfigMap configMap = new ConfigMapBuilder()
                .withNewMetadata()
                .withName("my-config")
                .endMetadata()
                .build();

        assertThat(name(configMap)).isEqualTo("my-config");
    }
}
