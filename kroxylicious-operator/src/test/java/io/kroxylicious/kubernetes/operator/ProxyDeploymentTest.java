/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kubernetes.operator;

import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

import static io.kroxylicious.kubernetes.operator.ProxyDeployment.KROXYLICIOUS_IMAGE_ENV_VAR;
import static org.assertj.core.api.Assertions.assertThat;

class ProxyDeploymentTest {

    @Test
    @ClearEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR)
    void operandImageDefault() {
        assertThat(ProxyDeployment.getOperandImage())
                .matches("^quay.io/kroxylicious/kroxylicious:.*");
    }

    @Test
    @SetEnvironmentVariable(key = KROXYLICIOUS_IMAGE_ENV_VAR, value = "quay.io/myorg/kroxylicious:1")
    void operandImageOverrideFromEnvironment() {
        assertThat(ProxyDeployment.getOperandImage())
                .isEqualTo("quay.io/myorg/kroxylicious:1");
    }
}