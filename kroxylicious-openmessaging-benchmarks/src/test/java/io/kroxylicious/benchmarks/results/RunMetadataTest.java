/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RunMetadataTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void generateCreatesMetadataFile(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        Path metadataFile = tempDir.resolve("run-metadata.json");
        assertThat(metadataFile).exists();
    }

    @Test
    void metadataContainsGitCommit(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("gitCommit"))
                .as("Metadata should contain gitCommit field")
                .isTrue();
    }

    @Test
    void metadataContainsGitBranch(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("gitBranch"))
                .as("Metadata should contain gitBranch field")
                .isTrue();
    }

    @Test
    void metadataContainsTimestamp(@TempDir Path tempDir) throws IOException {
        RunMetadata.generate(tempDir);

        JsonNode metadata = MAPPER.readTree(Files.readString(tempDir.resolve("run-metadata.json")));
        assertThat(metadata.has("timestamp"))
                .as("Metadata should contain timestamp field")
                .isTrue();
    }

    @Test
    void generateCreatesOutputDirectory(@TempDir Path tempDir) throws IOException {
        Path nested = tempDir.resolve("sub/dir");
        RunMetadata.generate(nested);

        assertThat(nested.resolve("run-metadata.json")).exists();
    }
}
