/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.tester;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.ServiceBasedPluginFactoryRegistry;
import io.kroxylicious.proxy.config2.FilesystemSnapshot;
import io.kroxylicious.proxy.config2.ProxyConfigParser;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Builds a config2-format directory structure in a temporary directory,
 * enabling integration tests to exercise the full parsing pipeline:
 * filesystem files → {@link FilesystemSnapshot} → {@link ProxyConfigParser} → {@link Configuration}.
 */
public class FilesystemSnapshotBuilder {

    private static final String PROXY_CONFIG_FILENAME = "proxy.yaml";
    private static final String PASSWORDS_FILENAME = "passwords.yaml";
    private static final String PLUGINS_DIR = "plugins.d";
    private static final String YAML_SUFFIX = ".yaml";

    private record PluginEntry(String yaml, @Nullable byte[] binaryData, @Nullable String dataExtension) {}

    private final Path baseDir;
    private String proxyConfigYaml;
    private final Map<String, Map<String, PluginEntry>> pluginsByInterface = new LinkedHashMap<>();
    private final Map<String, String> passwords = new LinkedHashMap<>();

    /**
     * Creates a builder that writes config2 files to the given directory.
     * The directory should typically come from a JUnit {@code @TempDir}.
     *
     * @param baseDir the directory to write configuration files into
     */
    public FilesystemSnapshotBuilder(Path baseDir) {
        this.baseDir = baseDir;
    }

    /**
     * Sets the proxy configuration as raw YAML.
     *
     * @param yaml the proxy.yaml content
     * @return this builder
     */
    public FilesystemSnapshotBuilder proxyConfig(String yaml) {
        this.proxyConfigYaml = yaml;
        return this;
    }

    /**
     * Generates a config2-format proxy.yaml for a single virtual cluster pointing at
     * the given bootstrap servers, with optional default filters.
     *
     * @param bootstrapServers the backing Kafka cluster bootstrap servers
     * @param defaultFilterNames names of filters to include in defaultFilters
     * @return this builder
     */
    public FilesystemSnapshotBuilder proxyConfig(
                                                 String bootstrapServers,
                                                 String... defaultFilterNames) {
        StringBuilder sb = new StringBuilder();
        sb.append("version: v1alpha1\n");
        sb.append("virtualClusters:\n");
        sb.append("  - name: demo\n");
        sb.append("    targetCluster:\n");
        sb.append("      bootstrapServers: ").append(bootstrapServers).append("\n");
        sb.append("    gateways:\n");
        sb.append("      - name: default\n");
        sb.append("        portIdentifiesNode:\n");
        sb.append("          bootstrapAddress: localhost:9192\n");
        if (defaultFilterNames.length > 0) {
            sb.append("defaultFilters:\n");
            for (String filterName : defaultFilterNames) {
                sb.append("  - ").append(filterName).append("\n");
            }
        }
        this.proxyConfigYaml = sb.toString();
        return this;
    }

    /**
     * Adds a YAML plugin instance.
     *
     * @param pluginInterface the plugin interface class
     * @param instanceName the plugin instance name (must match the {@code name} field in the YAML)
     * @param yaml the plugin instance YAML (must contain name, type, version fields)
     * @return this builder
     */
    public FilesystemSnapshotBuilder addPluginInstance(
                                                       Class<?> pluginInterface,
                                                       String instanceName,
                                                       String yaml) {
        return addPluginInstance(pluginInterface.getName(), instanceName, yaml);
    }

    /**
     * Adds a YAML plugin instance.
     *
     * @param pluginInterfaceName the fully qualified plugin interface class name
     * @param instanceName the plugin instance name (must match the {@code name} field in the YAML)
     * @param yaml the plugin instance YAML (must contain name, type, version fields)
     * @return this builder
     */
    public FilesystemSnapshotBuilder addPluginInstance(
                                                       String pluginInterfaceName,
                                                       String instanceName,
                                                       String yaml) {
        pluginsByInterface
                .computeIfAbsent(pluginInterfaceName, k -> new LinkedHashMap<>())
                .put(instanceName, new PluginEntry(yaml, null, null));
        return this;
    }

    /**
     * Adds a binary plugin instance (e.g. a keystore) with a sidecar data file.
     *
     * @param pluginInterfaceName the fully qualified plugin interface class name
     * @param instanceName the instance name
     * @param type the fully qualified plugin implementation class name
     * @param version the configuration version
     * @param fileExtension the file extension for the sidecar file (e.g. {@code ".p12"}, {@code ".jks"})
     * @param data the raw binary data
     * @return this builder
     */
    public FilesystemSnapshotBuilder addBinaryPluginInstance(
                                                             String pluginInterfaceName,
                                                             String instanceName,
                                                             String type,
                                                             String version,
                                                             String fileExtension,
                                                             byte[] data) {
        String metadataYaml = "name: " + instanceName + "\n"
                + "type: " + type + "\n"
                + "version: " + version + "\n";
        pluginsByInterface
                .computeIfAbsent(pluginInterfaceName, k -> new LinkedHashMap<>())
                .put(instanceName, new PluginEntry(metadataYaml, data, fileExtension));
        return this;
    }

    /**
     * Adds a password for a named resource.
     *
     * @param resourceName the resource name
     * @param password the password
     * @return this builder
     */
    public FilesystemSnapshotBuilder addPassword(
                                                 String resourceName,
                                                 String password) {
        passwords.put(resourceName, password);
        return this;
    }

    /**
     * Writes all configuration files to the base directory and returns the path.
     *
     * @return the base directory path
     */
    public Path buildPath() {
        try {
            writeProxyConfig();
            writePasswords();
            writePlugins();
            return baseDir;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to write config2 directory structure", e);
        }
    }

    /**
     * Writes all configuration files and returns a {@link FilesystemSnapshot}.
     *
     * @return a snapshot backed by the written directory
     */
    public FilesystemSnapshot buildSnapshot() {
        buildPath();
        return new FilesystemSnapshot(baseDir);
    }

    /**
     * Writes all configuration files, parses them via {@link ProxyConfigParser},
     * and returns the resulting {@link Configuration}.
     *
     * @return the parsed configuration
     */
    public Configuration buildConfiguration() {
        FilesystemSnapshot snapshot = buildSnapshot();
        ProxyConfigParser parser = new ProxyConfigParser(new ServiceBasedPluginFactoryRegistry());
        return parser.parse(snapshot);
    }

    private void writeProxyConfig() throws IOException {
        if (proxyConfigYaml == null) {
            throw new IllegalStateException("proxyConfig() must be called before building");
        }
        Files.writeString(baseDir.resolve(PROXY_CONFIG_FILENAME), proxyConfigYaml);
    }

    private void writePasswords() throws IOException {
        if (passwords.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder();
        for (var entry : passwords.entrySet()) {
            sb.append(entry.getKey()).append(": \"")
                    .append(entry.getValue().replace("\"", "\\\""))
                    .append("\"\n");
        }
        Files.writeString(baseDir.resolve(PASSWORDS_FILENAME), sb.toString());
    }

    private void writePlugins() throws IOException {
        if (pluginsByInterface.isEmpty()) {
            return;
        }
        Path pluginsDir = baseDir.resolve(PLUGINS_DIR);
        Files.createDirectories(pluginsDir);

        for (var interfaceEntry : pluginsByInterface.entrySet()) {
            Path interfaceDir = pluginsDir.resolve(interfaceEntry.getKey());
            Files.createDirectories(interfaceDir);

            for (var instanceEntry : interfaceEntry.getValue().entrySet()) {
                String instanceName = instanceEntry.getKey();
                PluginEntry plugin = instanceEntry.getValue();

                Files.writeString(
                        interfaceDir.resolve(instanceName + YAML_SUFFIX),
                        plugin.yaml(),
                        StandardCharsets.UTF_8);

                if (plugin.binaryData() != null) {
                    Files.write(
                            interfaceDir.resolve(instanceName + plugin.dataExtension()),
                            plugin.binaryData());
                }
            }
        }
    }

}
