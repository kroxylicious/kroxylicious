/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.File;
import java.io.IOException;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Deserialized view of a {@code run-metadata.json} file produced alongside each OMB result.
 * Only the fields needed for result analysis are mapped; all others are ignored.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record RunMetadataResult(
                                @JsonProperty("scenario") String scenario,
                                @JsonProperty("workload") String workload) {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /**
     * Reads the {@code run-metadata.json} file that sits alongside the given result file,
     * returning empty if the file does not exist or cannot be parsed.
     */
    public static Optional<RunMetadataResult> fromSiblingOf(File resultFile) {
        File metadataFile = new File(resultFile.getParentFile(), "run-metadata.json");
        if (!metadataFile.exists()) {
            return Optional.empty();
        }
        try {
            return Optional.of(MAPPER.readValue(metadataFile, RunMetadataResult.class));
        }
        catch (IOException e) {
            return Optional.empty();
        }
    }
}
