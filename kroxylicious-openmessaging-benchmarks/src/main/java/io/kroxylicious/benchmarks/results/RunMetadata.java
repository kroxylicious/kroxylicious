/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.benchmarks.results;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Generates a run-metadata.json file containing git and timestamp information
 * for an OMB benchmark run.
 */
public class RunMetadata {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter ISO_UTC = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss'Z'")
            .withZone(ZoneOffset.UTC);

    private RunMetadata() {
    }

    /**
     * Generates a run-metadata.json file in the given directory.
     *
     * @param outputDir the directory to write the metadata file to
     * @throws IOException if writing fails or git commands fail
     */
    public static void generate(Path outputDir) throws IOException {
        Files.createDirectories(outputDir);

        String gitCommit = execGitCommand("rev-parse", "HEAD");
        String gitBranch = execGitCommand("rev-parse", "--abbrev-ref", "HEAD");
        String timestamp = ISO_UTC.format(Instant.now());

        Map<String, String> metadata = Map.of(
                "gitCommit", gitCommit,
                "gitBranch", gitBranch,
                "timestamp", timestamp);

        Path metadataFile = outputDir.resolve("run-metadata.json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(metadataFile.toFile(), metadata);
    }

    private static String execGitCommand(String... args) throws IOException {
        String[] command = new String[args.length + 1];
        command[0] = "git";
        System.arraycopy(args, 0, command, 1, args.length);

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(true);
        Process process = pb.start();

        String output;
        try (InputStream is = process.getInputStream()) {
            output = new String(is.readAllBytes(), StandardCharsets.UTF_8).trim();
        }

        try {
            int exitCode = process.waitFor();
            if (exitCode != 0) {
                return "unknown";
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return "unknown";
        }

        return output;
    }
}
