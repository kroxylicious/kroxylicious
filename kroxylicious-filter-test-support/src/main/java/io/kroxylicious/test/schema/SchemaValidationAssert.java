/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.test.schema;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates raw plugin configuration against the plugin's JSON Schema.
 * Use this in plugin tests to verify that schemas are not stricter than
 * what the plugin's Java-level validation allows.
 */
public final class SchemaValidationAssert {

    private static final String SCHEMA_PATH_PREFIX = "META-INF/kroxylicious/schemas/";
    private static final YAMLMapper YAML_MAPPER = new YAMLMapper();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final JsonSchemaFactory SCHEMA_FACTORY = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V202012);

    private SchemaValidationAssert() {
    }

    /**
     * Asserts that the given raw configuration is accepted by the JSON Schema
     * for the specified plugin type and version. The schema is loaded from
     * {@code META-INF/kroxylicious/schemas/{pluginSimpleName}/{version}.schema.yaml}
     * on the current thread's context classloader.
     *
     * @param pluginSimpleName the unqualified plugin class name
     * @param version the config version (e.g. "v1alpha1")
     * @param rawConfig the raw configuration as a map, representing the YAML config object
     */
    public static void assertSchemaAccepts(
                                           String pluginSimpleName,
                                           String version,
                                           Map<String, ?> rawConfig) {
        JsonSchema schema = loadSchema(pluginSimpleName, version);
        JsonNode configNode = OBJECT_MAPPER.valueToTree(rawConfig);
        Set<ValidationMessage> errors = schema.validate(configNode);
        assertThat(errors)
                .as("schema for %s/%s should accept config %s", pluginSimpleName, version, rawConfig)
                .isEmpty();
    }

    private static JsonSchema loadSchema(
                                         String pluginSimpleName,
                                         String version) {
        String path = SCHEMA_PATH_PREFIX + pluginSimpleName + "/" + version + ".schema.yaml";
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            assertThat(is)
                    .as("schema should be on classpath at %s", path)
                    .isNotNull();
            JsonNode schemaNode = YAML_MAPPER.readTree(is);
            return SCHEMA_FACTORY.getSchema(schemaNode);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to load schema from " + path, e);
        }
    }
}
