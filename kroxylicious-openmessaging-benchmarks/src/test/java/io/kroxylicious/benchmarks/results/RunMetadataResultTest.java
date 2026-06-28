/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.fasterxml.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class RunMetadataResultTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    void deserializesScenarioAndWorkload() throws IOException {
        String json = """
                {"scenario": "baseline", "workload": "1topic-1kb"}
                """;
        RunMetadataResult result = MAPPER.readValue(json, RunMetadataResult.class);
        assertThat(result.scenario()).isEqualTo("baseline");
        assertThat(result.workload()).isEqualTo("1topic-1kb");
    }

    @Test
    void ignoresUnknownFields() throws IOException {
        String json = """
                {"scenario": "baseline", "workload": "1topic-1kb", "gitCommit": "abc", "unrelated": 42}
                """;
        assertThat(MAPPER.readValue(json, RunMetadataResult.class)).isNotNull();
    }

    @Test
    void returnsEmptyWhenNoMetadataFile(@TempDir Path tempDir) {
        File resultFile = tempDir.resolve("result.json").toFile();
        assertThat(RunMetadataResult.fromSiblingOf(resultFile)).isEmpty();
    }

    @Test
    void returnsMetadataFromSiblingFile(@TempDir Path tempDir) throws IOException {
        Files.writeString(tempDir.resolve("run-metadata.json"),
                """
                        {"scenario": "proxy-no-filters", "workload": "10topics-1kb"}
                        """);
        File resultFile = tempDir.resolve("result.json").toFile();

        assertThat(RunMetadataResult.fromSiblingOf(resultFile))
                .isPresent().get().satisfies(m -> {
                    assertThat(m.scenario()).isEqualTo("proxy-no-filters");
                    assertThat(m.workload()).isEqualTo("10topics-1kb");
                });
    }
}
