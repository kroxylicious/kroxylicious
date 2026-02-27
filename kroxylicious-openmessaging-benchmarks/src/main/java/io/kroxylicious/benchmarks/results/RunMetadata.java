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
import java.util.LinkedHashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

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

        String gitCommit = execCommand("git", "rev-parse", "HEAD");
        String gitBranch = execCommand("git", "rev-parse", "--abbrev-ref", "HEAD");
        String timestamp = ISO_UTC.format(Instant.now());

        Map<String, Object> metadata = new LinkedHashMap<>();
        metadata.put("gitCommit", gitCommit);
        metadata.put("gitBranch", gitBranch);
        metadata.put("timestamp", timestamp);

        Map<String, Object> minikubeProfile = minikubeProfileConfig();
        if (minikubeProfile != null) {
            metadata.put("minikubeProfile", minikubeProfile);
        }

        Path metadataFile = outputDir.resolve("run-metadata.json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(metadataFile.toFile(), metadata);
    }

    private static Map<String, Object> minikubeProfileConfig() {
        try {
            String json = execCommand("minikube", "profile", "list", "-o", "json");
            JsonNode root = MAPPER.readTree(json);
            JsonNode valid = root.path("valid");
            if (!valid.isArray() || valid.isEmpty()) {
                return null;
            }
            JsonNode profile = valid.get(0);
            JsonNode machine = profile.path("Config").path("MachineConfig");
            JsonNode k8s = profile.path("Config").path("KubernetesConfig");

            Map<String, Object> config = new LinkedHashMap<>();
            config.put("profile", profile.path("Name").asText("unknown"));
            config.put("driver", machine.path("Driver").asText("unknown"));
            config.put("cpus", machine.path("CPUs").asInt(0));
            config.put("memoryMb", machine.path("Memory").asInt(0));
            config.put("kubernetesVersion", k8s.path("KubernetesVersion").asText("unknown"));
            config.put("containerRuntime", k8s.path("ContainerRuntime").asText("unknown"));
            return config;
        }
        catch (Exception e) {
            // minikube not available or not configured — omit from metadata
            return null;
        }
    }

    @SuppressFBWarnings(value = "COMMAND_INJECTION", justification = "command arguments are hardcoded string literals, not user input")
    private static String execCommand(String command, String... args) throws IOException {
        String[] fullCommand = new String[args.length + 1];
        fullCommand[0] = command;
        System.arraycopy(args, 0, fullCommand, 1, args.length);

        ProcessBuilder pb = new ProcessBuilder(fullCommand);
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
