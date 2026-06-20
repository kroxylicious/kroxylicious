/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS org.yaml:snakeyaml:2.2

import java.io.*;
import java.nio.file.*;
import java.util.*;
import org.yaml.snakeyaml.Yaml;

public class concat_manifests {
    public static void main(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("Usage: jbang concat_manifests.java SOURCEDIR FULL_INSTALL_OUTPUT CRDS_ONLY_OUTPUT");
            System.exit(1);
        }

        var sourceDir = Path.of(args[0]);
        if (!Files.exists(sourceDir)) {
            System.out.println("[concat_manifests.java] SOURCEDIR '" + sourceDir + "' does not exist");
            System.exit(1);
        }
        var fullInstallOutput = Path.of(args[1]);
        var crdsOnlyOutput = Path.of(args[2]);

        var yaml = new Yaml();

        // Collect all YAML files sorted alphabetically (preserves numeric prefix ordering)
        List<Path> allManifests;
        try (var stream = Files.list(sourceDir)) {
            allManifests = stream
                    .filter(path -> path.toString().endsWith(".yaml") || path.toString().endsWith(".yml"))
                    .sorted()
                    .toList();
        }

        // Separate CRDs from other resources
        List<Path> crdManifests = new ArrayList<>();

        for (Path manifest : allManifests) {
            try (var reader = Files.newBufferedReader(manifest)) {
                if (yaml.load(reader) instanceof Map<?, ?> data) {
                    String kind = kind(data);
                    if ("CustomResourceDefinition".equals(kind)) {
                        crdManifests.add(manifest);
                    }
                }
            }
        }

        // Generate full install manifest (CRDs + other resources)
        System.out.println("[concat_manifests.java] Generating full install manifest: " + fullInstallOutput);
        writeManifest(fullInstallOutput, allManifests, "Install", extractVersion(fullInstallOutput));

        // Generate CRDs-only manifest
        System.out.println("[concat_manifests.java] Generating CRDs-only manifest: " + crdsOnlyOutput);
        writeManifest(crdsOnlyOutput, crdManifests, "CRDs", extractVersion(crdsOnlyOutput));
    }

    private static void writeManifest(Path output, List<Path> manifests, String type, String version) throws IOException {
        try (var writer = Files.newBufferedWriter(output)) {
            // Write header comment
            writer.write("#\n");
            writer.write("# Copyright Kroxylicious Authors.\n");
            writer.write("#\n");
            writer.write("# Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0\n");
            writer.write("#\n");
            writer.write("# Kroxylicious " + type + " Manifest\n");
            writer.write("# Version: " + version + "\n");
            writer.write("#\n");
            writer.write("# Installation:\n");
            writer.write("#   kubectl apply -f " + output.getFileName() + "\n");
            writer.write("#\n");
            writer.write("# Or via URL:\n");
            writer.write("#   kubectl apply -f https://github.com/kroxylicious/kroxylicious/releases/download/v" + version + "/" + output.getFileName() + "\n");
            writer.write("#\n");
            writer.write("---\n");

            // Write each manifest
            for (int i = 0; i < manifests.size(); i++) {
                Path manifest = manifests.get(i);
                String content = Files.readString(manifest);

                // Remove any leading --- if present
                content = content.replaceFirst("^---\\s*\\n", "");

                // Remove all leading comment/blank lines before the first YAML content
                // This strips license headers and file-specific notes while preserving
                // inline comments that are part of the resource definition
                content = content.replaceFirst("(?s)^(#.*?\\n|\\s*\\n)+(?=\\S)", "");

                writer.write(content);

                // Add separator between documents (but not after the last one)
                if (i < manifests.size() - 1) {
                    if (!content.endsWith("\n")) {
                        writer.write("\n");
                    }
                    writer.write("---\n");
                }
            }
        }
    }

    private static String kind(Map<?, ?> data) throws IOException {
        if (data.get("kind") instanceof String kind) {
            return kind;
        }
        throw new IOException("YAML lacks .kind, or it is not a string");
    }

    private static String extractVersion(Path outputPath) {
        // Extract version from filename like kroxylicious-operator-install-0.22.0-SNAPSHOT.yaml
        String filename = outputPath.getFileName().toString();
        String[] parts = filename.split("-");

        // Find the part that looks like a version (starts with digit)
        for (int i = parts.length - 1; i >= 0; i--) {
            String part = parts[i];
            if (part.matches("^\\d+.*")) {
                // Remove .yaml extension
                return part.replaceAll("\\.ya?ml$", "");
            }
        }

        return "unknown";
    }
}
