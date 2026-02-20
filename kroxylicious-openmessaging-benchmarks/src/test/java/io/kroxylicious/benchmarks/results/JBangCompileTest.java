/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Verifies that each JBang entry point compiles successfully in offline mode.
 * <p>
 * This catches missing {@code //SOURCES} or {@code //DEPS} directives that
 * would prevent the scripts from running via JBang. Uses {@code --offline}
 * so dependencies are resolved from the local Maven cache ({@code ~/.m2/repository})
 * without network access.
 */
@EnabledIf(value = "isJBangAvailable", disabledReason = "JBang is not installed or not available in PATH")
class JBangCompileTest {

    private static final String JBANG_EXECUTABLE = "jbang";
    private static final long PROCESS_TIMEOUT_SECONDS = 30;
    private static final Path GENERATED_SOURCES = Path.of(System.getProperty("jbang.generated.sources"));

    static boolean isJBangAvailable() {
        try {
            Process process = new ProcessBuilder(JBANG_EXECUTABLE, "version").start();
            boolean finished = process.waitFor(5, TimeUnit.SECONDS);
            return finished && process.exitValue() == 0;
        }
        catch (Exception e) {
            return false;
        }
    }

    @ParameterizedTest(name = "{0}")
    @ValueSource(strings = {
            "io/kroxylicious/benchmarks/results/CompareResults.java",
            "io/kroxylicious/benchmarks/results/CollectResults.java"
    })
    void jbangEntryPointCompiles(String entryPoint) {
        Path sourceFile = GENERATED_SOURCES.resolve(entryPoint);
        assertThat(sourceFile).exists();

        List<String> command = List.of(JBANG_EXECUTABLE, "--offline", "build", sourceFile.toString());
        String output = executeCommand(command);
        assertThat(output)
                .as("JBang build output for %s", entryPoint)
                .doesNotContain("error:");
    }

    private static String executeCommand(List<String> command) {
        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);

        try {
            Process process = pb.start();
            String output;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                output = reader.lines().collect(Collectors.joining("\n"));
            }

            boolean finished = process.waitFor(PROCESS_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            if (!finished) {
                process.destroyForcibly();
                fail("Command timed out after %ds: %s".formatted(PROCESS_TIMEOUT_SECONDS, String.join(" ", command)));
            }

            assertThat(process.exitValue())
                    .as("Exit code for: %s\nOutput:\n%s", String.join(" ", command), output)
                    .isZero();

            return output;
        }
        catch (Exception e) {
            return fail("Failed to execute command: %s".formatted(String.join(" ", command)), e);
        }
    }
}
