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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Utility class for executing Helm CLI commands.
 */
public class HelmUtils {

    private static final Path HELM_CHART_DIR = getHelmChartDirectory();

    private static Path getHelmChartDirectory() {
        String chartDirProperty = System.getProperty("helm.chart.directory");
        if (chartDirProperty != null) {
            return Paths.get(chartDirProperty);
        }
        return Paths.get("helm/kroxylicious-benchmark").toAbsolutePath();
    }

    /**
     * Runs helm lint on the chart.
     *
     * @return lint output
     * @throws IOException if helm command fails
     */
    public static String lint() throws IOException {
        List<String> command = List.of("helm", "lint", HELM_CHART_DIR.toString());
        return executeCommand(command);
    }

    /**
     * Validates that Helm is installed and available.
     *
     * @return true if Helm is available
     */
    public static boolean isHelmAvailable() {
        try {
            executeCommand(List.of("helm", "version"));
            return true;
        }
        catch (IOException e) {
            return false;
        }
    }

    /**
     * Executes a command and returns its output.
     *
     * @param command Command and arguments to execute
     * @return Command output
     * @throws IOException if command fails or times out
     */
    private static String executeCommand(List<String> command) throws IOException {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        Process process = pb.start();
        StringBuilder output = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line).append("\n");
            }
        }

        try {
            boolean finished = process.waitFor(30, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                throw new IOException("Command timed out: " + String.join(" ", command));
            }

            int exitCode = process.exitValue();
            if (exitCode != 0) {
                throw new IOException("Command failed with exit code " + exitCode + ": " + String.join(" ", command) + "\nOutput:\n" + output);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Command interrupted: " + String.join(" ", command), e);
        }

        return output.toString();
    }
}
