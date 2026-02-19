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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(strings = { "gitCommit", "gitBranch", "timestamp" })
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
}
