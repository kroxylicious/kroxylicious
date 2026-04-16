/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config2;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

/**
 * Validates plugin configuration against JSON Schema files shipped on the classpath.
 * Schemas are looked up at {@code META-INF/kroxylicious/schemas/{pluginSimpleName}/{version}.schema.yaml}.
 * Validation is opt-in: if no schema is found, the config is accepted without schema validation.
 */
class ConfigSchemaValidator {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigSchemaValidator.class);
    private static final String SCHEMA_PATH_PREFIX = "META-INF/kroxylicious/schemas/";

    private final YAMLMapper yamlMapper;
    private final JsonSchemaFactory schemaFactory;
    private final Map<String, Optional<JsonSchema>> schemaCache = new HashMap<>();

    ConfigSchemaValidator(YAMLMapper yamlMapper) {
        this.yamlMapper = yamlMapper;
        this.schemaFactory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);
    }

    /**
     * Validates the raw config against the schema for the given plugin type and version.
     * If no schema is found on the classpath, returns silently.
     * If validation fails, throws {@link IllegalArgumentException} listing all errors.
     *
     * @param pluginType the plugin implementation type name (may be fully qualified)
     * @param version the config version
     * @param instanceName the plugin instance name (for error messages)
     * @param rawConfig the raw config object (Maps/Lists/primitives from Jackson)
     */
    void validateIfSchemaAvailable(
                                   String pluginType,
                                   String version,
                                   String instanceName,
                                   Object rawConfig) {
        Optional<JsonSchema> schema = loadSchema(pluginType, version);
        if (schema.isEmpty()) {
            return;
        }
        JsonNode configNode = yamlMapper.valueToTree(rawConfig);
        Set<ValidationMessage> errors = schema.get().validate(configNode);
        if (!errors.isEmpty()) {
            String errorDetail = errors.stream()
                    .map(ValidationMessage::getMessage)
                    .collect(Collectors.joining("\n  - ", "\n  - ", ""));
            throw new IllegalArgumentException(
                    "Schema validation failed for plugin instance '" + instanceName
                            + "' (type '" + pluginType + "', version '" + version + "'):" + errorDetail);
        }
    }

    private Optional<JsonSchema> loadSchema(
                                            String pluginType,
                                            String version) {
        String simpleName = simpleName(pluginType);
        String cacheKey = simpleName + "/" + version;
        return schemaCache.computeIfAbsent(cacheKey, k -> doLoadSchema(simpleName, version));
    }

    private Optional<JsonSchema> doLoadSchema(
                                              String simpleName,
                                              String version) {
        String path = SCHEMA_PATH_PREFIX + simpleName + "/" + version + ".schema.yaml";
        URL resource = Thread.currentThread().getContextClassLoader().getResource(path);
        if (resource == null) {
            LOGGER.atDebug()
                    .addKeyValue("path", path)
                    .log("No schema found on classpath, skipping validation");
            return Optional.empty();
        }
        try (InputStream is = resource.openStream()) {
            JsonNode schemaNode = yamlMapper.readTree(is);
            JsonSchema schema = schemaFactory.getSchema(schemaNode);
            LOGGER.atDebug()
                    .addKeyValue("path", path)
                    .log("Loaded schema for validation");
            return Optional.of(schema);
        }
        catch (IOException e) {
            LOGGER.atWarn()
                    .addKeyValue("path", path)
                    .setCause(e)
                    .log("Failed to load schema, skipping validation");
            return Optional.empty();
        }
    }

    private static String simpleName(String pluginType) {
        int lastDot = pluginType.lastIndexOf('.');
        return lastDot >= 0 ? pluginType.substring(lastDot + 1) : pluginType;
    }
}
