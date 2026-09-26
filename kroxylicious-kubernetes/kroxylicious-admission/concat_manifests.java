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
        if (args.length != 4) {
            System.out.println("Usage: jbang concat_manifests.java SOURCEDIR FULL_INSTALL_OUTPUT CRDS_ONLY_OUTPUT VERSION");
            System.exit(1);
        }

        var sourceDir = Path.of(args[0]);
        if (!Files.exists(sourceDir)) {
            System.out.println("[concat_manifests.java] SOURCEDIR '" + sourceDir + "' does not exist");
            System.exit(1);
        }
        var fullInstallOutput = Path.of(args[1]);
        var crdsOnlyOutput = Path.of(args[2]);
        var version = args[3];

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
        writeManifest(fullInstallOutput, allManifests, "Install", version);

        // Generate CRDs-only manifest
        System.out.println("[concat_manifests.java] Generating CRDs-only manifest: " + crdsOnlyOutput);
        writeManifest(crdsOnlyOutput, crdManifests, "CRDs", version);
    }

    private static void writeManifest(Path output, List<Path> manifests, String type, String version) throws IOException {
        try (var writer = Files.newBufferedWriter(output)) {
            // Write header comment (no trailing --- needed, each document adds its own)
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

            // Write each manifest
            for (int i = 0; i < manifests.size(); i++) {
                Path manifest = manifests.get(i);
                String content = Files.readString(manifest);

                // Remove all leading comment/blank lines before the first YAML content
                // This strips license headers and file-specific notes while preserving
                // inline comments that are part of the resource definition
                content = content.replaceFirst("(?s)^(#.*?\\n|\\s*\\n)+(?=\\S)", "");

                // Remove any leading --- if present (after stripping comments)
                content = content.replaceFirst("^---\\s*\\n", "");

                // Add separator before each document
                writer.write("---\n");
                writer.write(content);

                // Ensure trailing newline
                if (!content.endsWith("\n")) {
                    writer.write("\n");
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
}
