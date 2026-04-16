/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * A {@link Snapshot} built programmatically for testing. No filesystem access required.
 */
public class TestSnapshot implements Snapshot {

    private static final YAMLMapper YAML_MAPPER = new YAMLMapper();

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MetadataOnly(String name, String type, String version, boolean shared) {}

    private final String proxyConfig;
    private final Map<String, Map<String, InstanceEntry>> pluginsByInterface;
    private final Map<String, char[]> passwords;

    private record InstanceEntry(PluginInstanceMetadata metadata, byte[] data) {}

    private TestSnapshot(
                         String proxyConfig,
                         Map<String, Map<String, InstanceEntry>> pluginsByInterface,
                         Map<String, char[]> passwords) {
        this.proxyConfig = proxyConfig;
        this.pluginsByInterface = pluginsByInterface;
        this.passwords = passwords;
    }

    @Override
    public String proxyConfig() {
        return proxyConfig;
    }

    @Override
    public List<String> pluginInterfaces() {
        return List.copyOf(pluginsByInterface.keySet());
    }

    @Override
    public List<String> pluginInstances(String pluginInterfaceName) {
        Map<String, InstanceEntry> instances = pluginsByInterface.get(pluginInterfaceName);
        if (instances == null) {
            return List.of();
        }
        return List.copyOf(instances.keySet());
    }

    @Override
    public PluginInstanceContent pluginInstance(
                                                String pluginInterfaceName,
                                                String pluginInstanceName) {
        InstanceEntry entry = requireEntry(pluginInterfaceName, pluginInstanceName);
        return new PluginInstanceContent(entry.metadata(), entry.data());
    }

    @Override
    @Nullable
    public char[] resourcePassword(String resourceName) {
        return passwords.get(resourceName);
    }

    private InstanceEntry requireEntry(String pluginInterfaceName, String pluginInstanceName) {
        Map<String, InstanceEntry> instances = pluginsByInterface.get(pluginInterfaceName);
        if (instances == null) {
            throw new IllegalArgumentException(
                    "No plugin instances for interface '" + pluginInterfaceName + "'");
        }
        InstanceEntry entry = instances.get(pluginInstanceName);
        if (entry == null) {
            throw new IllegalArgumentException(
                    "Plugin instance '" + pluginInstanceName + "' not found for interface '"
                            + pluginInterfaceName + "'");
        }
        return entry;
    }

    /**
     * Creates a new builder for constructing a TestSnapshot.
     *
     * @param proxyConfig the proxy configuration YAML
     * @return a new builder
     */
    public static Builder builder(String proxyConfig) {
        return new Builder(proxyConfig);
    }

    /** Builder for {@link TestSnapshot}. */
    public static class Builder {

        private final String proxyConfig;
        private final Map<String, Map<String, InstanceEntry>> pluginsByInterface = new HashMap<>();
        private final Map<String, char[]> passwords = new HashMap<>();
        private long nextGeneration = 1;

        private Builder(String proxyConfig) {
            this.proxyConfig = proxyConfig;
        }

        /**
         * Adds a YAML plugin instance configuration. The instance name and metadata
         * are extracted from the YAML content.
         *
         * @param pluginInterface the plugin interface class
         * @param yaml the plugin instance YAML configuration
         * @return this builder
         */
        public Builder addPluginInstance(
                                         Class<?> pluginInterface,
                                         String yaml) {
            return addPluginInstance(pluginInterface.getName(), yaml);
        }

        /**
         * Adds a YAML plugin instance configuration using the interface name as a string.
         * The instance name and metadata are extracted from the YAML content.
         *
         * @param pluginInterfaceName the fully qualified plugin interface class name
         * @param yaml the plugin instance YAML configuration
         * @return this builder
         */
        public Builder addPluginInstance(
                                         String pluginInterfaceName,
                                         String yaml) {
            MetadataOnly parsed = parseMetadata(yaml);
            String name = parsed.name();
            if (name == null) {
                throw new IllegalArgumentException(
                        "Plugin instance YAML must contain a 'name' field: " + yaml);
            }
            PluginInstanceMetadata metadata = new PluginInstanceMetadata(
                    name, parsed.type(), parsed.version(), parsed.shared(), nextGeneration++);
            byte[] data = yaml.getBytes(StandardCharsets.UTF_8);
            pluginsByInterface
                    .computeIfAbsent(pluginInterfaceName, k -> new HashMap<>())
                    .put(name, new InstanceEntry(metadata, data));
            return this;
        }

        /**
         * Adds a binary plugin instance (e.g. a keystore).
         *
         * @param pluginInterfaceName the fully qualified plugin interface class name
         * @param name the instance name
         * @param type the fully qualified plugin implementation class name
         * @param version the configuration version
         * @param data the raw binary data
         * @return this builder
         */
        public Builder addBinaryPluginInstance(
                                               String pluginInterfaceName,
                                               String name,
                                               String type,
                                               String version,
                                               byte[] data) {
            PluginInstanceMetadata metadata = new PluginInstanceMetadata(
                    name, type, version, false, nextGeneration++);
            pluginsByInterface
                    .computeIfAbsent(pluginInterfaceName, k -> new HashMap<>())
                    .put(name, new InstanceEntry(metadata, data));
            return this;
        }

        /**
         * Adds a password for a named resource.
         *
         * @param resourceName the resource name
         * @param password the password
         * @return this builder
         */
        public Builder addPassword(String resourceName, String password) {
            passwords.put(resourceName, password.toCharArray());
            return this;
        }

        private static MetadataOnly parseMetadata(String yaml) {
            try {
                return YAML_MAPPER.readValue(yaml, MetadataOnly.class);
            }
            catch (IOException e) {
                throw new UncheckedIOException("Failed to parse plugin instance YAML", e);
            }
        }

        /** Builds the TestSnapshot. */
        public TestSnapshot build() {
            return new TestSnapshot(proxyConfig, pluginsByInterface, passwords);
        }
    }
}
