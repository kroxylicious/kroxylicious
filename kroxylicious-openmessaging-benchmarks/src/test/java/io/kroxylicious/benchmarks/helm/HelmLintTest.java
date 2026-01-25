/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.helm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Tests that verify the Helm chart passes linting validation.
 */
class HelmLintTest {

    @Test
    void testHelmLintPasses() throws IOException, InterruptedException {
        // Given: Helm is available
        assumeTrue(isHelmAvailable(), "Helm is not installed or not available in PATH");

        // When: Running helm lint
        String chartPath = getChartDirectory().toString();
        ProcessBuilder pb = new ProcessBuilder("helm", "lint", chartPath);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        boolean finished = process.waitFor(30, TimeUnit.SECONDS);
        if (!finished) {
            process.destroyForcibly();
            fail("helm lint command timed out");
        }

        int exitCode = process.exitValue();

        // Then: Should pass with no errors
        if (exitCode != 0) {
            fail("helm lint failed with exit code " + exitCode + ":\n" + output);
        }

        String result = output.toString();
        assertTrue(result.contains("1 chart(s) linted"), "Expected successful lint output");
        assertTrue(result.contains("0 chart(s) failed"), "Expected no chart failures");
    }

    private boolean isHelmAvailable() {
        try {
            Process process = new ProcessBuilder("helm", "version").start();
            return process.waitFor(5, TimeUnit.SECONDS) && process.exitValue() == 0;
        }
        catch (IOException | InterruptedException e) {
            return false;
        }
    }

    private Path getChartDirectory() {
        String chartDirProperty = System.getProperty("helm.chart.directory");
        if (chartDirProperty != null) {
            return Paths.get(chartDirProperty);
        }
        return Paths.get("helm/kroxylicious-benchmark").toAbsolutePath();
    }
}
