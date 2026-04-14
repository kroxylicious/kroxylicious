/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the {@code classpath()} function in {@code kroxylicious-start.sh}.
 */
class StartScriptTest {

    private static final Path START_SCRIPT = Path.of("src/assembly/kroxylicious-start.sh");

    @Test
    void classpathIncludesPluginSubdirectories(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("plugins/plugin-a"));
        Files.createDirectories(tempDir.resolve("plugins/plugin-b"));

        String output = runClasspathFunction(tempDir);

        assertThat(output)
                .contains("/libs/*")
                .contains("/plugins/plugin-a/")
                .contains("/plugins/plugin-b/");
    }

    @Test
    void classpathWorksWithoutPluginsDirectory(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));

        String output = runClasspathFunction(tempDir);

        assertThat(output)
                .contains("/libs/*")
                .doesNotContain("plugins");
    }

    @Test
    void classpathWorksWithEmptyPluginsDirectory(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("plugins"));

        String output = runClasspathFunction(tempDir);

        assertThat(output)
                .contains("/libs/*")
                .doesNotContain("plugins");
    }

    @Test
    void classpathPreservesKroxyliciousClasspathEnvVar(@TempDir Path tempDir) throws Exception {
        Files.createDirectories(tempDir.resolve("bin"));
        Files.createDirectories(tempDir.resolve("libs"));
        Files.createDirectories(tempDir.resolve("plugins/my-plugin"));

        String command = classpathTestCommand(tempDir);
        var pb = new ProcessBuilder("bash", "-c", command);
        pb.environment().put("KROXYLICIOUS_CLASSPATH", "/extra/jars/*");
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        assertThat(p.waitFor(5, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).isZero();

        assertThat(output)
                .contains("/libs/*")
                .contains("/plugins/my-plugin/")
                .contains("/extra/jars/*");
    }

    private String runClasspathFunction(Path baseDir) throws Exception {
        String command = classpathTestCommand(baseDir);
        Process p = new ProcessBuilder("bash", "-c", command)
                .redirectErrorStream(true)
                .start();
        String output = new String(p.getInputStream().readAllBytes(), StandardCharsets.UTF_8).trim();
        assertThat(p.waitFor(5, TimeUnit.SECONDS)).isTrue();
        assertThat(p.exitValue()).isZero();
        return output;
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
