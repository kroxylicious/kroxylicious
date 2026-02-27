/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunMetadataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String FIXTURE_GIT_COMMIT = "abc123def456abc123def456abc123def456abc123";
    private static final String FIXTURE_GIT_BRANCH = "feat/test-branch";

    // --- Integration-style tests (real CommandRunner) ---

    @Test
    void generateCreatesMetadataFile(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        assertThat(tempDir.resolve("run-metadata.json")).exists();
    }

    @ParameterizedTest
    @ValueSource(strings = { "gitCommit", "gitBranch", "timestamp", "hostSystem" })
    void metadataContainsExpectedField(String fieldName, @TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has(fieldName))
                .as("Metadata should contain %s field", fieldName)
                .isTrue();
    }

    @Test
    void generateCreatesOutputDirectory(@TempDir Path tempDir) throws IOException {
        Path nested = tempDir.resolve("sub/dir");
        RunMetadata.generate(nested);

        assertThat(nested.resolve("run-metadata.json")).exists();
    }

    @Test
    void timestampIsIsoUtcFormat(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.get("timestamp").asText())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");
    }

    @ParameterizedTest
    @ValueSource(strings = { "os", "osVersion", "osArch", "logicalCpus" })
    void hostSystemContainsCrossPlatformFields(String fieldName, @TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode hostSystem = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json"))).get("hostSystem");
        assertThat(hostSystem.has(fieldName))
                .as("hostSystem should contain %s", fieldName)
                .isTrue();
    }

    @Test
    void hostSystemOsFieldsMatchSystemProperties(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode hostSystem = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json"))).get("hostSystem");
        assertThat(hostSystem.get("os").asText()).isEqualTo(System.getProperty("os.name"));
        assertThat(hostSystem.get("osVersion").asText()).isEqualTo(System.getProperty("os.version"));
        assertThat(hostSystem.get("osArch").asText()).isEqualTo(System.getProperty("os.arch"));
    }

    @Test
    void hostSystemLogicalCpuCountIsPositive(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode hostSystem = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json"))).get("hostSystem");
        assertThat(hostSystem.get("logicalCpus").asInt()).isPositive();
    }

    // --- CommandRunner injection tests ---

    @Test
    void generateInvokesGitRevParseForCommitAndBranch(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn(FIXTURE_GIT_COMMIT);
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");

        RunMetadata.generate(tempDir, runner);

        ArgumentCaptor<String[]> argsCaptor = ArgumentCaptor.forClass(String[].class);
        verify(runner, times(2)).run(eq("git"), argsCaptor.capture());
        assertThat(argsCaptor.getAllValues())
                .anySatisfy(args -> assertThat(args).containsExactly("rev-parse", "HEAD"))
                .anySatisfy(args -> assertThat(args).containsExactly("rev-parse", "--abbrev-ref", "HEAD"));
    }

    @Test
    void generatePopulatesGitFieldsFromRunner(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class)))
                .thenReturn(FIXTURE_GIT_COMMIT, FIXTURE_GIT_BRANCH);
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.get("gitCommit").asText()).isEqualTo(FIXTURE_GIT_COMMIT);
        assertThat(metadata.get("gitBranch").asText()).isEqualTo(FIXTURE_GIT_BRANCH);
    }

    @Test
    void minikubeProfilePopulatedFromRunnerOutput(@TempDir Path tempDir) throws IOException, URISyntaxException {
        String minikubeJson = Files.readString(fixture("minikube-profile-list.json"));
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn(minikubeJson);

        RunMetadata.generate(tempDir, runner);

        JsonNode profile = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json"))).get("minikubeProfile");
        assertThat(profile).isNotNull();
        assertThat(profile.get("profile").asText()).isEqualTo("minikube");
        assertThat(profile.get("driver").asText()).isEqualTo("podman");
        assertThat(profile.get("cpus").asInt()).isEqualTo(20);
        assertThat(profile.get("memoryMb").asInt()).isEqualTo(30000);
        assertThat(profile.get("kubernetesVersion").asText()).isEqualTo("v1.34.0");
        assertThat(profile.get("containerRuntime").asText()).isEqualTo("containerd");
    }

    @Test
    void minikubeProfileAbsentWhenRunnerReturnsEmptyJson(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("minikubeProfile")).isFalse();
    }

    // --- parseProcEntries tests ---

    @Test
    void parseProcEntriesExtractsCpuModel() throws IOException, URISyntaxException {
        var info = RunMetadata.parseProcEntries(fixture("proc/cpuinfo"), fixture("proc/meminfo"));

        assertThat(info.get("cpuModel")).isEqualTo("Intel(R) Xeon(R) CPU E5-2643 v3 @ 3.40GHz");
    }

    @Test
    void parseProcEntriesExtractsCpuMhz() throws IOException, URISyntaxException {
        var info = RunMetadata.parseProcEntries(fixture("proc/cpuinfo"), fixture("proc/meminfo"));

        assertThat(info).containsKey("cpuMhz");
    }

    @Test
    void parseProcEntriesExtractsTotalMemoryGb() throws IOException, URISyntaxException {
        var info = RunMetadata.parseProcEntries(fixture("proc/cpuinfo"), fixture("proc/meminfo"));

        assertThat(info.get("totalMemoryGb")).isEqualTo(31L);
    }

    @Test
    void parseProcEntriesReturnsEmptyMapForMissingFiles(@TempDir Path tempDir) throws IOException {
        var info = RunMetadata.parseProcEntries(
                tempDir.resolve("nonexistent-cpuinfo"),
                tempDir.resolve("nonexistent-meminfo"));

        assertThat(info).isEmpty();
    }

    private Path fixture(String name) throws URISyntaxException {
        return Path.of(getClass().getResource("/fixtures/" + name).toURI());
    }
}
