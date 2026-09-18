/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.kroxylicious.migrations.cli;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import picocli.CommandLine;

import static org.assertj.core.api.Assertions.assertThat;

class ConvertConfigCommandTest {

    private static final String DEPRECATED_FORM = """
            management:
              endpoints:
                prometheus: {}
            virtualClusters:
              - name: demo
                targetCluster:
                  bootstrapServers: localhost:9092
            """;

    private static final String CURRENT_FORM = """
            management:
              endpoints:
                prometheus: {}
            clusterDefinitions:
              - name: demo-target
                bootstrapServers: localhost:9092
            virtualClusters:
              - name: demo
                target:
                  cluster: demo-target
            """;

    @TempDir
    Path configDir;

    private StringWriter out;
    private CommandLine commandLine;

    @BeforeEach
    void setUp() {
        out = new StringWriter();
        commandLine = new CommandLine(new KroxyliciousMigrations());
        commandLine.setOut(new PrintWriter(out));
    }

    @Test
    void shouldConvertConfigurationUsingDeprecatedForm() throws Exception {
        // Given
        Path configFile = writeConfig("proxy-config.yaml", DEPRECATED_FORM);

        // When
        int exitCode = commandLine.execute("convert-config", configFile.toString());

        // Then
        assertThat(exitCode).isZero();
        assertThat(configFile).hasContent(CURRENT_FORM);
        assertThat(out.toString()).contains("Converted 1 of 1 file(s).");
    }

    @Test
    void shouldConvertEachGivenFile() throws Exception {
        // Given
        Path first = writeConfig("first.yaml", DEPRECATED_FORM);
        Path second = writeConfig("second.yaml", DEPRECATED_FORM);

        // When
        int exitCode = commandLine.execute("convert-config", first.toString(), second.toString());

        // Then
        assertThat(exitCode).isZero();
        assertThat(first).hasContent(CURRENT_FORM);
        assertThat(second).hasContent(CURRENT_FORM);
        assertThat(out.toString()).contains("Converted 2 of 2 file(s).");
    }

    @Test
    void shouldReportButNotWriteChangesOnDryRun() throws Exception {
        // Given
        Path configFile = writeConfig("proxy-config.yaml", DEPRECATED_FORM);

        // When
        int exitCode = commandLine.execute("convert-config", "--dry-run", configFile.toString());

        // Then
        assertThat(exitCode).isZero();
        assertThat(configFile).hasContent(DEPRECATED_FORM);
        assertThat(out.toString())
                .contains("Would convert 1 of 1 file(s).")
                .contains("+clusterDefinitions:")
                .contains("-    targetCluster:");
    }

    @Test
    void shouldReportNoChangesForConfigurationAlreadyConverted() throws Exception {
        // Given
        Path configFile = writeConfig("proxy-config.yaml", CURRENT_FORM);

        // When
        int exitCode = commandLine.execute("convert-config", configFile.toString());

        // Then
        assertThat(exitCode).isZero();
        assertThat(configFile).hasContent(CURRENT_FORM);
        assertThat(out.toString()).contains("No changes required.");
    }

    @Test
    void shouldRejectFileWhichDoesNotExist() {
        // Given
        Path configFile = configDir.resolve("absent.yaml");

        // When
        int exitCode = commandLine.execute("convert-config", configFile.toString());

        // Then
        assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
        assertThat(configFile).doesNotExist();
    }

    @Test
    void shouldPrintUsageWhenNoCommandGiven() {
        // When
        int exitCode = commandLine.execute();

        // Then
        assertThat(exitCode).isEqualTo(CommandLine.ExitCode.USAGE);
        assertThat(out.toString()).contains("convert-config");
    }

    private Path writeConfig(String fileName, String content) throws Exception {
        return Files.writeString(configDir.resolve(fileName), content);
    }
}
