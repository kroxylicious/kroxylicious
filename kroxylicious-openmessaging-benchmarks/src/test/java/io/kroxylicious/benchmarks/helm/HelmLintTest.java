/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.IOException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that verify the Helm chart passes linting validation.
 */
class HelmLintTest {

    @BeforeAll
    static void checkHelmAvailable() {
        assumeTrue(HelmUtils.isHelmAvailable(), "Helm is not installed or not available in PATH");
    }

    @Test
    void testHelmLintPasses() throws IOException {
        // When: Running helm lint
        String lintOutput = HelmUtils.lint();

        // Then: Should pass with no errors
        assertTrue(lintOutput.contains("1 chart(s) linted"), "Expected successful lint output");
        assertTrue(lintOutput.contains("0 chart(s) failed"), "Expected no chart failures");
    }
}
