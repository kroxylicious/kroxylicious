/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.net.URL;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Karapace Schema Registry.
 */
public record KarapaceSchemaValidationConfig(URL karapaceRegistryUrl, int schemaId) {
    /**
     * Construct KarapaceSchemaValidationConfig
     * @param schemaId Karapace schema ID to be used for schema validation
     * @param karapaceRegistryUrl Karapace Schema Registry instance url
     */
    @JsonCreator
    public KarapaceSchemaValidationConfig(@JsonProperty(value = "karapaceRegistryUrl", required = true) URL karapaceRegistryUrl,
                                          @JsonProperty(value = "schemaId", required = true) int schemaId) {
        this.schemaId = schemaId;
        this.karapaceRegistryUrl = karapaceRegistryUrl;
    }

    /**
     * @return the configured schema ID to be used
     */
    @Override
    public int schemaId() {
        return schemaId;
    }

    /**
     * @return the karapace registry url to be used for this validation
     */
    @Override
    public URL karapaceRegistryUrl() {
        return karapaceRegistryUrl;
    }

    @Override
    public String toString() {
        return "KarapaceSchemaValidationConfig{" +
                "schemaId=" + schemaId +
                ", karapaceRegistryUrl='" + karapaceRegistryUrl + '\'' +
                '}';
    }
}
