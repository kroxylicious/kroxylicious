/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link Snapshot} backed by a filesystem directory structure.
 * <p>Expected layout:</p>
 * <pre>
 * baseDir/
 *   proxy.yaml
 *   passwords.yaml              (optional — resource name → password map)
 *   plugins.d/
 *     io.kroxylicious.proxy.filter.FilterFactory/
 *       my-encryption.yaml
 *     io.kroxylicious.proxy.tls.KeyMaterialProvider/
 *       my-keystore.yaml        (metadata: name, type, version)
 *       my-keystore.data        (binary keystore bytes)
 * </pre>
 */
public class FilesystemSnapshot implements Snapshot {

    private static final String PROXY_CONFIG_FILENAME = "proxy.yaml";
    private static final String PASSWORDS_FILENAME = "passwords.yaml";
    private static final String PLUGINS_DIR = "plugins.d";
    private static final String YAML_SUFFIX = ".yaml";
    private static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MetadataOnly(String name, String type, String version, boolean shared) {}

    private final Path baseDir;
    private volatile Map<String, char[]> passwords;

    /**
     * Creates a snapshot from the given base directory.
     *
     * @param baseDir the directory containing proxy.yaml and plugins.d/
     */
    public FilesystemSnapshot(Path baseDir) {
        this.baseDir = baseDir;
    }

    @Override
    public String proxyConfig() {
        try {
            return Files.readString(baseDir.resolve(PROXY_CONFIG_FILENAME));
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read " + PROXY_CONFIG_FILENAME + " from " + baseDir, e);
        }
    }

    @Override
    public List<String> pluginInterfaces() {
        Path pluginsDir = baseDir.resolve(PLUGINS_DIR);
        if (!Files.isDirectory(pluginsDir)) {
            return List.of();
        }
        try (Stream<Path> dirs = Files.list(pluginsDir)) {
            return dirs
                    .filter(Files::isDirectory)
                    .map(p -> Objects.requireNonNull(p.getFileName()).toString())
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to list plugin interfaces in " + pluginsDir, e);
        }
    }

    @Override
    public List<String> pluginInstances(String pluginInterfaceName) {
        Path interfaceDir = baseDir.resolve(PLUGINS_DIR).resolve(pluginInterfaceName);
        if (!Files.isDirectory(interfaceDir)) {
            return List.of();
        }
        try (Stream<Path> files = Files.list(interfaceDir)) {
            return files
                    .filter(p -> p.toString().endsWith(YAML_SUFFIX))
                    .map(p -> {
                        String filename = Objects.requireNonNull(p.getFileName()).toString();
                        return filename.substring(0, filename.length() - YAML_SUFFIX.length());
                    })
                    .toList();
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to list plugin instances in " + interfaceDir, e);
        }
    }

    @Override
    public PluginInstanceContent pluginInstance(
                                                String pluginInterfaceName,
                                                String pluginInstanceName) {
        Path yamlFile = instanceYamlPath(pluginInterfaceName, pluginInstanceName);
        try {
            // Read the YAML metadata and compute generation from both files' mtimes
            byte[] yamlBytes = Files.readAllBytes(yamlFile);
            MetadataOnly parsed = YAML_MAPPER.readValue(yamlBytes, MetadataOnly.class);
            validateName(pluginInstanceName, yamlFile, parsed.name());

            long generation = Files.getLastModifiedTime(yamlFile).toMillis();
            byte[] data;
            Path sidecarFile = findSidecarFile(pluginInterfaceName, pluginInstanceName);
            if (sidecarFile != null) {
                // Binary resource — sidecar file contains the actual data
                data = Files.readAllBytes(sidecarFile);
                long dataGeneration = Files.getLastModifiedTime(sidecarFile).toMillis();
                generation = Math.max(generation, dataGeneration);
            }
            else {
                // YAML resource — the YAML file itself is the data
                data = yamlBytes;
            }

            var metadata = new PluginInstanceMetadata(
                    parsed.name(), parsed.type(), parsed.version(), parsed.shared(), generation);
            return new PluginInstanceContent(metadata, data);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    "Plugin instance '" + pluginInstanceName + "' not found for interface '"
                            + pluginInterfaceName + "' at " + yamlFile,
                    e);
        }
    }

    @Override
    @Nullable
    public char[] resourcePassword(String resourceName) {
        return loadPasswords().get(resourceName);
    }

    private Map<String, char[]> loadPasswords() {
        if (passwords == null) {
            Path passwordsFile = baseDir.resolve(PASSWORDS_FILENAME);
            if (!Files.exists(passwordsFile)) {
                passwords = Map.of();
            }
            else {
                try {
                    Map<String, String> raw = YAML_MAPPER.readValue(
                            passwordsFile.toFile(),
                            new TypeReference<Map<String, String>>() {
                            });
                    Map<String, char[]> result = new HashMap<>();
                    raw.forEach((k, v) -> result.put(k, v.toCharArray()));
                    passwords = result;
                }
                catch (IOException e) {
                    throw new UncheckedIOException("Failed to read " + PASSWORDS_FILENAME, e);
                }
            }
        }
        return passwords;
    }

    private Path instanceYamlPath(String pluginInterfaceName, String pluginInstanceName) {
        return baseDir
                .resolve(PLUGINS_DIR)
                .resolve(pluginInterfaceName)
                .resolve(pluginInstanceName + YAML_SUFFIX);
    }

    /**
     * Finds a sidecar (non-YAML) file for the given plugin instance.
     * The sidecar file must share the same stem as the YAML file but have a different extension
     * (e.g. {@code my-keystore.p12}, {@code my-truststore.jks}).
     * At most one sidecar file may exist per plugin instance.
     *
     * @return the sidecar file path, or {@code null} if none exists
     * @throws IllegalArgumentException if multiple sidecar files exist for the same instance
     */
    @edu.umd.cs.findbugs.annotations.Nullable
    private Path findSidecarFile(String pluginInterfaceName, String pluginInstanceName) {
        Path interfaceDir = baseDir.resolve(PLUGINS_DIR).resolve(pluginInterfaceName);
        String prefix = pluginInstanceName + ".";
        String yamlFilename = pluginInstanceName + YAML_SUFFIX;
        try (Stream<Path> files = Files.list(interfaceDir)) {
            List<Path> sidecars = files
                    .filter(p -> {
                        String filename = Objects.requireNonNull(p.getFileName()).toString();
                        return filename.startsWith(prefix) && !filename.equals(yamlFilename);
                    })
                    .toList();
            if (sidecars.size() > 1) {
                throw new IllegalArgumentException(
                        "Plugin instance '" + pluginInstanceName + "' has multiple sidecar files: "
                                + sidecars.stream()
                                        .map(p -> Objects.requireNonNull(p.getFileName()).toString())
                                        .toList()
                                + ". At most one non-YAML file per plugin instance is allowed.");
            }
            return sidecars.isEmpty() ? null : sidecars.get(0);
        }
        catch (IOException e) {
            throw new UncheckedIOException(
                    "Failed to list files in " + interfaceDir, e);
        }
    }

    private static void validateName(String expectedName, Path file, String actualName) {
        if (actualName == null) {
            throw new IllegalArgumentException(
                    "Plugin instance file " + file + " does not contain a 'name' field");
        }
        if (!expectedName.equals(actualName)) {
            throw new IllegalArgumentException(
                    "Plugin instance file " + file + " has name '" + actualName
                            + "' but filename implies '" + expectedName + "'");
        }
    }
}
