/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.ConstructorDetector;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.DurationSerde;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.config.PluginFactoryRegistry;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookService;
import io.kroxylicious.proxy.plugin.HasPluginReferences;
import io.kroxylicious.proxy.plugin.ResolvedPluginRegistry;
import io.kroxylicious.proxy.plugin.ResourceType;
import io.kroxylicious.proxy.plugin.Stateless;
import io.kroxylicious.proxy.service.HostPort;

/**
 * Parses proxy configuration from a {@link Snapshot}, supporting both legacy (unversioned)
 * and new-style (versioned, per-file) configuration formats.
 */
public class ProxyConfigParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConfigParser.class);

    private final YAMLMapper yamlMapper = (YAMLMapper) new YAMLMapper()
            .findAndRegisterModules()
            .registerModule(new SimpleModule()
                    .addSerializer(HostPort.class, new ToStringSerializer())
                    .addSerializer(java.time.Duration.class, new DurationSerde.Serializer())
                    .addDeserializer(java.time.Duration.class, new DurationSerde.Deserializer()))
            .setVisibility(PropertyAccessor.ALL, Visibility.NONE)
            .setVisibility(PropertyAccessor.FIELD, Visibility.ANY)
            .setVisibility(PropertyAccessor.CREATOR, Visibility.ANY)
            .setConstructorDetector(ConstructorDetector.USE_PROPERTIES_BASED)
            .enable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
            .enable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY)
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .setDefaultPropertyInclusion(JsonInclude.Include.NON_DEFAULT);
    private final PluginFactoryRegistry pluginFactoryRegistry;
    private final ConfigSchemaValidator schemaValidator;

    public ProxyConfigParser(PluginFactoryRegistry pluginFactoryRegistry) {
        this.pluginFactoryRegistry = pluginFactoryRegistry;
        this.schemaValidator = new ConfigSchemaValidator(yamlMapper);
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    record MaybeVersionedConfiguration(String version) {}

    /** Used to extract just the config section from a full YAML plugin instance file. */
    @JsonIgnoreProperties(ignoreUnknown = true)
    record ConfigSection(Object config) {}

    /**
     * Parses configuration from the given snapshot.
     *
     * @param snapshot the configuration snapshot
     * @return the parsed configuration
     */
    public Configuration parse(Snapshot snapshot) {
        String proxyYaml = snapshot.proxyConfig();
        try {
            var maybeVersioned = yamlMapper.readValue(proxyYaml, MaybeVersionedConfiguration.class);
            if (maybeVersioned.version() == null) {
                return new ConfigParser().parseConfiguration(proxyYaml);
            }
            return resolveVersioned(snapshot);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse proxy configuration", e);
        }
    }

    private static final String FILTER_FACTORY_INTERFACE = FilterFactory.class.getName();
    private static final String MICROMETER_INTERFACE = MicrometerConfigurationHookService.class.getName();

    /**
     * Resolves a versioned (config2) configuration from the snapshot.
     * Loads all plugin instances, deserialises their configs using version-aware
     * type resolution, validates references, computes initialisation order,
     * and builds a {@link Configuration} for the existing runtime.
     */
    private Configuration resolveVersioned(Snapshot snapshot) throws IOException {
        // Load all plugin instance configs from the snapshot
        Map<String, Map<String, ResolvedPluginConfig>> resolved = loadAllPluginConfigs(snapshot);

        // Validate proxy config references (defaultFilters, micrometer, virtual cluster filters)
        ProxyConfig proxyConfig = yamlMapper.readValue(snapshot.proxyConfig(), ProxyConfig.class);
        resolveProxyRefs(proxyConfig, resolved);

        // Build dependency graph, validate cross-file references, detect cycles,
        // and compute initialisation order (dependencies before dependents)
        List<ResolvedPluginConfig> initOrder = DependencyGraph.resolve(resolved);

        LOGGER.atInfo()
                .addKeyValue("pluginInterfaces", resolved.keySet())
                .addKeyValue("totalInstances", initOrder.size())
                .log("Resolved versioned configuration");

        // Build resolved plugin registry for non-filter plugins
        ResolvedPluginRegistry registry = ResolvedPluginRegistryImpl.build(
                initOrder, pluginFactoryRegistry, FILTER_FACTORY_INTERFACE);

        return buildConfiguration(proxyConfig, resolved, registry);
    }

    /**
     * Builds a {@link Configuration} from the resolved config2 state.
     * FilterFactory configs become {@link NamedFilterDefinition} objects,
     * MicrometerConfigurationHookService configs become {@link MicrometerDefinition} objects,
     * and the remaining proxy-level fields are passed through directly.
     */
    private Configuration buildConfiguration(
                                             ProxyConfig proxyConfig,
                                             Map<String, Map<String, ResolvedPluginConfig>> resolved,
                                             ResolvedPluginRegistry resolvedPluginRegistry) {

        List<NamedFilterDefinition> filterDefinitions = toFilterDefinitions(resolved);
        List<MicrometerDefinition> micrometerDefinitions = toMicrometerDefinitions(resolved);
        List<String> defaultFilters = proxyConfig.defaultFilters() == null
                ? null
                : proxyConfig.defaultFilters().stream()
                        .map(FilterFactoryRef::pluginInstance)
                        .toList();

        return new Configuration(
                proxyConfig.management(),
                filterDefinitions.isEmpty() ? null : filterDefinitions,
                defaultFilters,
                proxyConfig.virtualClusters(),
                micrometerDefinitions.isEmpty() ? null : micrometerDefinitions,
                proxyConfig.useIoUring(),
                proxyConfig.development() == null ? Optional.empty() : proxyConfig.development(),
                proxyConfig.network(),
                resolvedPluginRegistry);
    }

    private static List<NamedFilterDefinition> toFilterDefinitions(
                                                                   Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        Map<String, ResolvedPluginConfig> filters = resolved.get(FILTER_FACTORY_INTERFACE);
        if (filters == null) {
            return List.of();
        }
        return filters.values().stream()
                .map(rpc -> new NamedFilterDefinition(
                        rpc.pluginInstanceName(),
                        rpc.pc().type(),
                        rpc.pc().config()))
                .toList();
    }

    private static List<MicrometerDefinition> toMicrometerDefinitions(
                                                                      Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        Map<String, ResolvedPluginConfig> micros = resolved.get(MICROMETER_INTERFACE);
        if (micros == null) {
            return List.of();
        }
        return micros.values().stream()
                .map(rpc -> new MicrometerDefinition(rpc.pc().type(), rpc.pc().config()))
                .toList();
    }

    /**
     * Loads all plugin instance configurations from the snapshot, deserialising
     * each config object using the version-aware config type from the plugin's
     * {@code @Plugin} annotations.
     */
    private Map<String, Map<String, ResolvedPluginConfig>> loadAllPluginConfigs(Snapshot snapshot) throws IOException {
        Map<String, Map<String, ResolvedPluginConfig>> result = new HashMap<>();

        for (String pluginInterfaceName : snapshot.pluginInterfaces()) {
            Map<String, ResolvedPluginConfig> instances = new HashMap<>();
            for (String instanceName : snapshot.pluginInstances(pluginInterfaceName)) {
                PluginInstanceContent content = snapshot.pluginInstance(
                        pluginInterfaceName, instanceName);
                ResolvedPluginConfig resolvedConfig = resolvePluginConfig(
                        pluginInterfaceName, instanceName, content.metadata(), content.data());
                instances.put(instanceName, resolvedConfig);
            }
            result.put(pluginInterfaceName, instances);
        }
        return result;
    }

    /**
     * Resolves a single plugin instance's configuration by looking up the config type
     * from the plugin's {@code @Plugin} annotations and deserialising the raw config
     * object into the correct type.
     */
    private ResolvedPluginConfig resolvePluginConfig(
                                                     String pluginInterfaceName,
                                                     String instanceName,
                                                     PluginInstanceMetadata metadata,
                                                     byte[] data) {
        try {
            // Require fully qualified type names so plugin files are self-describing
            if (metadata.type() == null || !metadata.type().contains(".")) {
                throw new IllegalArgumentException(
                        "Plugin instance '" + instanceName + "' has type '" + metadata.type()
                                + "' which is not a fully qualified class name. "
                                + "Use the fully qualified class name of the plugin implementation.");
            }

            Class<?> pluginInterface = Class.forName(pluginInterfaceName);
            PluginFactory<?> factory = pluginFactoryRegistry.pluginFactory(pluginInterface);

            // Look up config type using version-aware resolution
            String version = metadata.version();
            Map<String, Class<?>> versions = factory.configVersions(metadata.type());
            Class<?> configType = versions.get(version);
            if (configType == null) {
                throw new IllegalArgumentException(
                        "Plugin '" + metadata.type() + "' does not support config version '" + version
                                + "'. Supported versions: " + versions.keySet());
            }

            // Versioned config types must implement HasPluginReferences
            if (!version.isEmpty() && !HasPluginReferences.class.isAssignableFrom(configType)) {
                throw new IllegalArgumentException(
                        "Versioned config type '" + configType.getName()
                                + "' for plugin '" + metadata.type()
                                + "' (version '" + version
                                + "') must implement " + HasPluginReferences.class.getSimpleName()
                                + " to declare its dependencies.");
            }

            // Validate shared lifecycle policy
            if (metadata.shared()) {
                Class<?> implType = factory.implementationType(metadata.type());
                if (!implType.isAnnotationPresent(Stateless.class)) {
                    throw new IllegalArgumentException(
                            "Plugin instance '" + instanceName + "' (type '" + metadata.type()
                                    + "') declares shared: true but the implementation is not @Stateless. "
                                    + "Only @Stateless plugins may be shared.");
                }
            }

            // Check if this plugin uses a binary resource format
            Class<?> implType = factory.implementationType(metadata.type());
            Object typedConfig;
            if (implType.isAnnotationPresent(ResourceType.class)) {
                // Binary resource — the data bytes are not YAML.
                // Deserialisation is deferred to plugin initialisation via @ResourceType serde.
                // Store the raw bytes as the config for now.
                typedConfig = data;
            }
            else {
                // YAML resource — parse the data bytes to extract the config section
                String yaml = new String(data, StandardCharsets.UTF_8);

                // Validate config against JSON Schema if available on classpath
                ConfigSection configSection = yamlMapper.readValue(yaml, ConfigSection.class);
                schemaValidator.validateIfSchemaAvailable(
                        metadata.type(), version, instanceName, configSection.config());

                // Deserialise the raw config into the typed config object
                typedConfig = yamlMapper.convertValue(configSection.config(), configType);
            }

            return new ResolvedPluginConfig(
                    pluginInterfaceName, instanceName,
                    new PluginConfig(instanceName, metadata.type(), version, metadata.shared(), typedConfig));
        }
        catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Plugin interface class not found: " + pluginInterfaceName, e);
        }
        catch (IOException e) {
            throw new IllegalArgumentException(
                    "Failed to parse plugin instance '" + instanceName + "' YAML data", e);
        }
    }

    /**
     * Validates that plugin references from the proxy config (defaultFilters, micrometer,
     * virtual cluster filters, etc.) point to existing plugin instances in the snapshot.
     */
    private void resolveProxyRefs(
                                  ProxyConfig proxyConfig,
                                  Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        proxyConfig.pluginReferences()
                .forEach(ref -> validateRefExists(ref.type(), ref.name(), resolved));
    }

    private void validateRefExists(
                                   String pluginInterfaceName,
                                   String instanceName,
                                   Map<String, Map<String, ResolvedPluginConfig>> resolved) {
        Map<String, ResolvedPluginConfig> instances = resolved.get(pluginInterfaceName);
        if (instances == null || !instances.containsKey(instanceName)) {
            throw new IllegalArgumentException(
                    "Plugin instance '" + instanceName + "' not found for interface '" + pluginInterfaceName + "'");
        }
    }

    /**
     * Returns the resolved plugin configurations. Useful for inspection and testing.
     *
     * @param snapshot the configuration snapshot
     * @return map of plugin interface name to map of instance name to resolved config
     */
    public Map<String, Map<String, ResolvedPluginConfig>> resolveAll(Snapshot snapshot) {
        try {
            return loadAllPluginConfigs(snapshot);
        }
        catch (IOException e) {
            throw new IllegalArgumentException("Failed to resolve plugin configurations", e);
        }
    }
}
