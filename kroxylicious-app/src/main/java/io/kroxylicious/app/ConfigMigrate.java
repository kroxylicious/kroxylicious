/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.app;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Parameter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import io.kroxylicious.proxy.config.ConfigParser;
import io.kroxylicious.proxy.config.Configuration;
import io.kroxylicious.proxy.config.MicrometerDefinition;
import io.kroxylicious.proxy.config.NamedFilterDefinition;
import io.kroxylicious.proxy.config.PluginFactory;
import io.kroxylicious.proxy.filter.FilterFactory;
import io.kroxylicious.proxy.micrometer.MicrometerConfigurationHookService;
import io.kroxylicious.proxy.plugin.PluginImplConfig;
import io.kroxylicious.proxy.plugin.PluginImplName;

import edu.umd.cs.findbugs.annotations.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

/**
 * CLI subcommand that migrates a legacy single-file proxy configuration into
 * the config2 multi-file format with separate plugin instance files.
 */
@Command(name = "migrate-config", description = "Migrate a legacy proxy configuration to config2 multi-file format.")
public class ConfigMigrate implements Callable<Integer> {

    private static final String FILTER_FACTORY_INTERFACE = FilterFactory.class.getName();
    private static final String MICROMETER_INTERFACE = MicrometerConfigurationHookService.class.getName();

    @Spec
    private @Nullable CommandSpec spec;

    @Option(names = { "-i", "--input" }, description = "Path to the legacy config YAML file", required = true)
    private @Nullable File inputFile;

    @Option(names = { "-o", "--output-dir" }, description = "Output directory for the migrated config", required = true)
    private @Nullable File outputDir;

    private final YAMLMapper yamlMapper;
    private final ObjectMapper objectMapper;

    /** Constructs the migration command with default mappers. */
    public ConfigMigrate() {
        this.yamlMapper = (YAMLMapper) YAMLMapper.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build()
                .findAndRegisterModules()
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        this.objectMapper = new ObjectMapper().findAndRegisterModules();
    }

    @Override
    public Integer call() throws Exception {
        Objects.requireNonNull(spec, "spec");
        Objects.requireNonNull(inputFile, "inputFile");
        Objects.requireNonNull(outputDir, "outputDir");

        if (!inputFile.exists()) {
            spec.commandLine().getErr().println("Input file does not exist: " + inputFile.getAbsolutePath());
            return 1;
        }

        Path outPath = outputDir.toPath();
        if (Files.exists(outPath)) {
            try (Stream<Path> entries = Files.list(outPath)) {
                if (entries.findAny().isPresent()) {
                    spec.commandLine().getErr().println("Output directory is not empty: " + outPath.toAbsolutePath());
                    return 1;
                }
            }
        }
        else {
            Files.createDirectories(outPath);
        }

        ConfigParser configParser = new ConfigParser();
        Configuration config;
        try (InputStream stream = Files.newInputStream(inputFile.toPath())) {
            config = configParser.parseConfiguration(stream);
        }

        // Also read raw YAML for pass-through serialisation of proxy.yaml
        @SuppressWarnings("unchecked")
        Map<String, Object> rawConfig = yamlMapper.readValue(inputFile, Map.class);

        Path pluginsDir = outPath.resolve("plugins.d");

        // Migrate filter definitions
        List<NamedFilterDefinition> filterDefs = config.filterDefinitions();
        if (filterDefs != null) {
            for (NamedFilterDefinition filterDef : filterDefs) {
                migrateFilter(configParser, filterDef, pluginsDir);
            }
        }

        // Migrate micrometer definitions
        List<String> micrometerNames = new ArrayList<>();
        List<MicrometerDefinition> micrometerDefs = config.micrometer();
        if (micrometerDefs != null) {
            for (int i = 0; i < micrometerDefs.size(); i++) {
                MicrometerDefinition md = micrometerDefs.get(i);
                String instanceName = generateMicrometerName(md.type(), i, micrometerDefs.size());
                micrometerNames.add(instanceName);
                migrateMicrometer(configParser, md, instanceName, pluginsDir);
            }
        }

        // Write proxy.yaml
        writeProxyYaml(rawConfig, micrometerNames, outPath);

        spec.commandLine().getOut().println("Migration complete. Output written to: " + outPath.toAbsolutePath());
        return 0;
    }

    private void migrateFilter(ConfigParser configParser,
                               NamedFilterDefinition filterDef,
                               Path pluginsDir)
            throws IOException {
        String filterName = filterDef.name();
        String filterType = filterDef.type();
        Object filterConfig = filterDef.config();

        PluginFactory<?> filterFactory = configParser.pluginFactory(FilterFactory.class);
        Map<String, Class<?>> versions = filterFactory.configVersions(filterType);

        // Resolve to FQCN — legacy configs may use short names
        String filterFqcn = filterFactory.implementationType(filterType).getName();

        // Find legacy config type and versioned config version
        Class<?> legacyConfigType = findLegacyConfigType(versions);
        String versionedVersion = findVersionedVersion(versions);

        // Discover nested plugin pairs via reflection on the legacy config type
        List<NestedPluginPair> nestedPlugins = discoverNestedPlugins(legacyConfigType);

        // Convert filter config to a raw map for manipulation
        @SuppressWarnings("unchecked")
        Map<String, Object> configMap = filterConfig != null
                ? objectMapper.convertValue(filterConfig, Map.class)
                : new LinkedHashMap<>();

        // Extract nested plugins and build versioned config
        Map<String, Object> versionedConfig = new LinkedHashMap<>();
        for (NestedPluginPair pair : nestedPlugins) {
            Object nameValue = configMap.remove(pair.nameField);
            Object configValue = configMap.remove(pair.configField);

            if (nameValue == null) {
                // Nullable plugin reference (e.g. SaslInspection.subjectBuilder) — skip
                continue;
            }

            String pluginTypeName = nameValue.toString();
            String instanceName = filterName + "-" + pair.nameField;
            String pluginInterfaceName = pair.pluginInterface.getName();

            // Resolve nested plugin type to FQCN
            String nestedFqcn = resolveToFqcn(configParser, pair.pluginInterface, pluginTypeName);

            // Determine version for the nested plugin
            String nestedVersion = determineNestedPluginVersion(configParser, pair.pluginInterface, pluginTypeName);

            // Write the nested plugin file
            writePluginFile(pluginsDir, pluginInterfaceName, instanceName,
                    nestedFqcn, nestedVersion, configValue);

            // Add instance name reference to the versioned config
            versionedConfig.put(pair.nameField, instanceName);
        }

        // Carry over non-plugin fields
        versionedConfig.putAll(configMap);

        // Write the filter plugin file with versioned config
        writePluginFile(pluginsDir, FILTER_FACTORY_INTERFACE, filterName,
                filterFqcn, versionedVersion, versionedConfig.isEmpty() ? null : versionedConfig);
    }

    private void migrateMicrometer(ConfigParser configParser,
                                   MicrometerDefinition md,
                                   String instanceName,
                                   Path pluginsDir)
            throws IOException {
        PluginFactory<?> micrometerFactory = configParser.pluginFactory(MicrometerConfigurationHookService.class);
        Map<String, Class<?>> versions = micrometerFactory.configVersions(md.type());
        String version = findVersionedVersion(versions);
        String fqcn = micrometerFactory.implementationType(md.type()).getName();

        writePluginFile(pluginsDir, MICROMETER_INTERFACE, instanceName,
                fqcn, version, md.config());
    }

    private void writePluginFile(Path pluginsDir,
                                 String pluginInterfaceName,
                                 String instanceName,
                                 String type,
                                 String version,
                                 @Nullable Object config)
            throws IOException {
        Path dir = pluginsDir.resolve(pluginInterfaceName);
        Files.createDirectories(dir);
        Path file = dir.resolve(instanceName + ".yaml");

        Map<String, Object> content = new LinkedHashMap<>();
        content.put("name", instanceName);
        content.put("type", type);
        content.put("version", version);
        if (config != null) {
            content.put("config", config);
        }

        yamlMapper.writeValue(file.toFile(), content);
    }

    private void writeProxyYaml(Map<String, Object> rawConfig,
                                List<String> micrometerNames,
                                Path outPath)
            throws IOException {
        Map<String, Object> proxy = new LinkedHashMap<>();
        proxy.put("version", "v1alpha1");

        // Copy fields from the raw config, stripping filterDefinitions (migrated to plugin files)
        for (var entry : rawConfig.entrySet()) {
            switch (entry.getKey()) {
                case "filterDefinitions":
                    // Omit — migrated to plugins.d/
                    break;
                case "micrometer":
                    // Replace with instance name list
                    if (!micrometerNames.isEmpty()) {
                        proxy.put("micrometer", micrometerNames);
                    }
                    break;
                default:
                    proxy.put(entry.getKey(), entry.getValue());
                    break;
            }
        }

        // Add micrometer names even if original config had no micrometer section
        if (!micrometerNames.isEmpty() && !proxy.containsKey("micrometer")) {
            proxy.put("micrometer", micrometerNames);
        }

        Path proxyFile = outPath.resolve("proxy.yaml");
        yamlMapper.writeValue(proxyFile.toFile(), proxy);
    }

    /**
     * Discovers @PluginImplName/@PluginImplConfig pairs on a config record type.
     * Uses canonical constructor parameters because these annotations target PARAMETER, not RECORD_COMPONENT.
     */
    static List<NestedPluginPair> discoverNestedPlugins(Class<?> configType) {
        if (!configType.isRecord()) {
            return List.of();
        }

        List<NestedPluginPair> pairs = new ArrayList<>();
        Parameter[] params = configType.getDeclaredConstructors()[0].getParameters();

        for (Parameter param : params) {
            PluginImplName nameAnno = param.getAnnotation(PluginImplName.class);
            if (nameAnno == null) {
                continue;
            }

            // Find the matching @PluginImplConfig parameter
            String nameField = param.getName();
            String configField = null;
            for (Parameter other : params) {
                PluginImplConfig configAnno = other.getAnnotation(PluginImplConfig.class);
                if (configAnno != null && configAnno.implNameProperty().equals(nameField)) {
                    configField = other.getName();
                    break;
                }
            }

            pairs.add(new NestedPluginPair(nameField, configField, nameAnno.value()));
        }

        return pairs;
    }

    private String resolveToFqcn(ConfigParser configParser,
                                 Class<?> pluginInterface,
                                 String pluginTypeName) {
        try {
            PluginFactory<?> factory = configParser.pluginFactory(pluginInterface);
            return factory.implementationType(pluginTypeName).getName();
        }
        catch (Exception e) {
            // If we can't resolve (e.g. plugin not on classpath), keep the original name
            return pluginTypeName;
        }
    }

    private String determineNestedPluginVersion(ConfigParser configParser,
                                                Class<?> pluginInterface,
                                                String pluginTypeName) {
        try {
            PluginFactory<?> factory = configParser.pluginFactory(pluginInterface);
            Map<String, Class<?>> versions = factory.configVersions(pluginTypeName);
            return findVersionedVersion(versions);
        }
        catch (Exception e) {
            // If we can't introspect versions (e.g. plugin not on classpath as a service),
            // fall back to v1alpha1
            return "v1alpha1";
        }
    }

    /**
     * Finds the legacy config type — the one with version key "" or the sole entry.
     */
    private static Class<?> findLegacyConfigType(Map<String, Class<?>> versions) {
        Class<?> legacy = versions.get("");
        if (legacy != null) {
            return legacy;
        }
        if (versions.size() == 1) {
            return versions.values().iterator().next();
        }
        // Multiple versions, none is legacy — pick the first
        return versions.values().iterator().next();
    }

    /**
     * Finds the best versioned config version string (non-empty preferred).
     */
    static String findVersionedVersion(Map<String, Class<?>> versions) {
        // Prefer non-empty versions
        for (String v : versions.keySet()) {
            if (!v.isEmpty()) {
                return v;
            }
        }
        // All versions are empty string — use v1alpha1 as default
        return "v1alpha1";
    }

    static String generateMicrometerName(String type, int index, int total) {
        // Convert simple class name to kebab-case
        String simpleName = type.contains(".") ? type.substring(type.lastIndexOf('.') + 1) : type;
        String kebab = toKebabCase(simpleName);
        return total > 1 ? kebab + "-" + index : kebab;
    }

    static String toKebabCase(String camelCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c) && i > 0) {
                sb.append('-');
            }
            sb.append(Character.toLowerCase(c));
        }
        return sb.toString();
    }

    record NestedPluginPair(String nameField, @Nullable String configField, Class<?> pluginInterface) {}
}
