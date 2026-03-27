/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
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
    private static Path cpuInfoPath;
    private static Path memInfoPath;
    private static Path actual;
    private static String minikubeJson;
    private static String kubectlGetNodesJson;
    private static String generatedJson;

    // --- Integration-style tests (real CommandRunner) ---

    @BeforeAll
    static void runGenerate(@TempDir Path tempDir) throws IOException, URISyntaxException {
        RunMetadata.generate(tempDir);
        actual = tempDir.resolve("run-metadata.json");
        assertThat(actual).exists()
                .isReadable();
        generatedJson = Files.readString(actual);

        cpuInfoPath = fixture("proc/cpuinfo");
        memInfoPath = fixture("proc/meminfo");
        minikubeJson = Files.readString(fixture("minikube-profile-list.json"));
        kubectlGetNodesJson = Files.readString(fixture("kubectl-get-nodes.json"));
    }

    @Test
    void generateCreatesMetadataFile() {
        assertThat(actual).exists();
    }

    @ParameterizedTest
    @ValueSource(strings = { "gitCommit", "gitBranch", "timestamp", "orchestratorSystem" })
    void metadataContainsExpectedField(String fieldName) throws IOException {
        JsonNode metadata = MAPPER.readTree(generatedJson);
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
    void timestampIsIsoUtcFormat() throws IOException {
        JsonNode metadata = MAPPER.readTree(generatedJson);
        assertThat(metadata.get("timestamp").asText())
                .matches("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z");
    }

    @ParameterizedTest
    @ValueSource(strings = { "os", "osVersion", "osArch", "logicalCpus" })
    void orchestratorSystemContainsCrossPlatformFields(String fieldName) throws IOException {
        JsonNode orchestratorSystem = MAPPER.readTree(generatedJson).get("orchestratorSystem");
        assertThat(orchestratorSystem.has(fieldName))
                .as("orchestratorSystem should contain %s", fieldName)
                .isTrue();
    }

    @Test
    void orchestratorSystemOsFieldsMatchSystemProperties() throws IOException {
        JsonNode orchestratorSystem = MAPPER.readTree(generatedJson).get("orchestratorSystem");
        assertThat(orchestratorSystem.get("os").asText()).isEqualTo(System.getProperty("os.name"));
        assertThat(orchestratorSystem.get("osVersion").asText()).isEqualTo(System.getProperty("os.version"));
        assertThat(orchestratorSystem.get("osArch").asText()).isEqualTo(System.getProperty("os.arch"));
    }

    @Test
    void orchestratorSystemLogicalCpuCountIsPositive() throws IOException {
        JsonNode orchestratorSystem = MAPPER.readTree(generatedJson).get("orchestratorSystem");
        assertThat(orchestratorSystem.get("logicalCpus").asInt()).isPositive();
    }

    // --- CommandRunner injection tests ---

    @Test
    void generateInvokesGitRevParseForCommitAndBranch(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn(FIXTURE_GIT_COMMIT);
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

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
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.get("gitCommit").asText()).isEqualTo(FIXTURE_GIT_COMMIT);
        assertThat(metadata.get("gitBranch").asText()).isEqualTo(FIXTURE_GIT_BRANCH);
    }

    @Test
    void minikubeProfilePopulatedFromRunnerOutput(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn(minikubeJson);
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

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
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("minikubeProfile")).isFalse();
    }

    @Test
    void clusterNodesPopulatedFromKubectlOutput(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn(kubectlGetNodesJson);

        RunMetadata.generate(tempDir, runner);

        JsonNode clusterNodes = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json"))).get("clusterNodes");
        assertThat(clusterNodes).isNotNull();
        assertThat(clusterNodes.get("nodeCount").asInt()).isEqualTo(3);
        assertThat(clusterNodes.get("arch").asText()).isEqualTo("amd64");
        assertThat(clusterNodes.get("osImage").asText()).isEqualTo("Red Hat Enterprise Linux CoreOS 416.94.202411260420-0");
        assertThat(clusterNodes.get("kernelVersion").asText()).isEqualTo("5.14.0-427.13.1.el9_4.x86_64");
        assertThat(clusterNodes.get("kubeletVersion").asText()).isEqualTo("v1.29.3+elf5e57");
        assertThat(clusterNodes.get("cpuPerNode").asText()).isEqualTo("16");
        assertThat(clusterNodes.get("memoryPerNodeGb").asLong()).isEqualTo(30L);
    }

    @Test
    void clusterNodesAbsentWhenKubectlFails(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("clusterNodes")).isFalse();
    }

    // --- parseProcEntries tests ---

    @Test
    void parseProcEntriesExtractsCpuModel() throws IOException {
        // Given
        var info = RunMetadata.parseProcCpuInfo(cpuInfoPath);

        assertThat(info).containsEntry("cpuModel", "Intel(R) Xeon(R) CPU E5-2643 v3 @ 3.40GHz");
    }

    @Test
    void parseProcEntriesExtractsCpuMhz() throws IOException {
        var info = RunMetadata.parseProcCpuInfo(cpuInfoPath);

        assertThat(info).containsKey("cpuMhz");
    }

    @Test
    void parseProcEntriesExtractsTotalMemoryGb() throws IOException {
        var info = RunMetadata.parseProcMemInfo(memInfoPath);

        assertThat(info).containsEntry("totalMemoryGb", 31L);
    }

    @Test
    void parseProcEntriesReturnsEmptyMapForMissingFiles(@TempDir Path tempDir) {
        var info = RunMetadata.parseProcEntries(
                tempDir.resolve("nonexistent-cpuinfo"),
                tempDir.resolve("nonexistent-meminfo"));

        assertThat(info).isEmpty();
    }

    private static Path fixture(String name) throws URISyntaxException {
        URL fixtureUrl = RunMetadataTest.class.getResource("/fixtures/" + name);
        Assumptions.assumeTrue(fixtureUrl != null);
        return Path.of(fixtureUrl.toURI());
    }

    // --- Probe context tests ---

    @Test
    void probeContextFieldsWrittenToMetadata(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

        Map<String, Object> probeContext = Map.of(
                "scenario", "proxy-no-filters",
                "workload", "1topic-1kb",
                "targetRate", 50000);

        RunMetadata.generate(tempDir, probeContext, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.get("scenario").asText()).isEqualTo("proxy-no-filters");
        assertThat(metadata.get("workload").asText()).isEqualTo("1topic-1kb");
        assertThat(metadata.get("targetRate").asInt()).isEqualTo(50000);
    }

    @Test
    void generateWithoutProbeContextOmitsProbeFields(@TempDir Path tempDir) throws IOException {
        RunMetadata.CommandRunner runner = mock(RunMetadata.CommandRunner.class);
        when(runner.run(eq("git"), any(String[].class))).thenReturn("unknown");
        when(runner.run(eq("minikube"), any(String[].class))).thenReturn("{}");
        when(runner.run(eq("kubectl"), any(String[].class))).thenReturn("unknown");

        RunMetadata.generate(tempDir, runner);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("scenario")).isFalse();
        assertThat(metadata.has("workload")).isFalse();
        assertThat(metadata.has("targetRate")).isFalse();
    }
}
