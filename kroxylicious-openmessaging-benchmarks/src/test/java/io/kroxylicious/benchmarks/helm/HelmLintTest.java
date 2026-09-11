/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.IOException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests that verify the Helm chart passes linting validation.
 */
@EnabledIf(value = "io.kroxylicious.benchmarks.helm.HelmUtils#isHelmAvailable", disabledReason = "Helm is not installed or not available in PATH")
class HelmLintTest {

    @Test
    void testHelmLintPasses() throws IOException {
        // When: Running helm lint
        String lintOutput = HelmUtils.lint();

        // Then: Should pass with no errors
        assertThat(lintOutput)
                .as("Helm lint should complete successfully")
                .containsPattern("\\d+ chart\\(s\\) linted")
                .contains("0 chart(s) failed");
    }
}
