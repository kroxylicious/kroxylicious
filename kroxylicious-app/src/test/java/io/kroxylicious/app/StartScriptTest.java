/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code classpath()} function in {@code kroxylicious-start.sh}.
 */
class StartScriptTest {

    private static final Path START_SCRIPT = Path.of("src/assembly/kroxylicious-start.sh");
    public static final String CLASSPATH_PLUGINS_WARNING = "WARNING: Loading Kroxylicious plugins from the 'classpath-plugins' directory is an Alpha feature: This feature may change, or be removed entirely, without warning in any future Kroxylicious release.";

    @Test
    void classpathIncludesPluginSubdirectories(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("classpath-plugins/plugin-a"));
        Files.createDirectories(tempDir.resolve("classpath-plugins/plugin-b"));

        var stdStreams = runClasspathFunction(tempDir);

        assertThat(stdStreams.standardError())
                .isEqualTo(CLASSPATH_PLUGINS_WARNING);

        assertThat(stdStreams.standardOutput())
                .contains("/libs/*")
                .contains("/classpath-plugins/plugin-a/")
                .contains("/classpath-plugins/plugin-b/");
    }

    @Test
    void classpathWorksWithoutPluginsDirectory(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));

        var stdStreams = runClasspathFunction(tempDir);

        assertThat(stdStreams.standardError())
                .as("No warning logged when directory is absent")
                .isEmpty();

        assertThat(stdStreams.standardOutput())
                .contains("/libs/*")
                .doesNotContain("classpath-plugins");

    }

    @Test
    void classpathWorksWithEmptyPluginsDirectory(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("classpath-plugins"));

        var stdStreams = runClasspathFunction(tempDir);

        assertThat(stdStreams.standardError())
                .as("No warning logged when directory is present but empty")
                .isEmpty();

        assertThat(stdStreams.standardOutput())
                .contains("/libs/*")
                .doesNotContain("classpath-plugins");
    }

    @Test
    void classpathPreservesKroxyliciousClasspathEnvVar(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("classpath-plugins/my-plugin"));

        var stdStreams = runClasspathFunction(tempDir, Map.of("KROXYLICIOUS_CLASSPATH", "/extra/jars/*"));

        assertThat(stdStreams.standardError())
                .isEqualTo(CLASSPATH_PLUGINS_WARNING);

        assertThat(stdStreams.standardOutput())
                .contains("/libs/*")
                .contains("/classpath-plugins/my-plugin/")
                .contains("/extra/jars/*");
    }

    record StdStreams(String standardOutput, String standardError) {}

    private StdStreams runClasspathFunction(Path baseDir) throws Exception {
        return runClasspathFunction(baseDir, Map.of());
    }

    private StdStreams runClasspathFunction(Path baseDir, Map<String, String> env) throws Exception {
        String command = classpathTestCommand(baseDir);
        Path err = baseDir.resolve("err");
        Path out = baseDir.resolve("out");

        ProcessBuilder pb = new ProcessBuilder("bash", "-c", command)
                .redirectError(err.toFile())
                .redirectOutput(out.toFile());
        pb.environment().putAll(env);
        Process p = pb.start();
        assertThat(p.waitFor(5, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).isZero();
        var error = Files.readString(err, StandardCharsets.UTF_8).trim();
        var output = Files.readString(out, StandardCharsets.UTF_8).trim();
        Files.delete(err);
        return new StdStreams(output, error);
    }

    private static String classpathTestCommand(Path baseDir) {
        // Override script_dir() to point at our temp directory's bin/,
        // then extract the real classpath() function from the production script via awk
        return """
                script_dir() { echo '%s'; }
                eval "$(awk '/^classpath\\(\\)/,/^}/' '%s')"
                classpath
                """.formatted(
                baseDir.resolve("bin"),
                START_SCRIPT.toAbsolutePath());
    }
}
