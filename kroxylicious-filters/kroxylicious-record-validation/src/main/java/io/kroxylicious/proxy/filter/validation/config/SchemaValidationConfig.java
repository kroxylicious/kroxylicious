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
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 */
public record SchemaValidationConfig(URL apicurioRegistryUrl, long apicurioGlobalId) {
    /**
     * Construct SchemaValidationConfig
     * @param apicurioGlobalId apicurio registry version global identifier to be used for schema validation
     * @param apicurioRegistryUrl Apicurio Registry instance url
     */
    @JsonCreator
    public SchemaValidationConfig(
            @JsonProperty(value = "apicurioRegistryUrl", required = true)
            URL apicurioRegistryUrl,
            @JsonProperty(value = "apicurioGlobalId", required = true)
            long apicurioGlobalId
    ) {
        this.apicurioGlobalId = apicurioGlobalId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
    }

    /**
     * @return the configured globalId to be used
     */
    @Override
    public long apicurioGlobalId() {
        return apicurioGlobalId;
    }

    /**
     * @return the apicurio registry url to be used for this validation
     */
    @Override
    public URL apicurioRegistryUrl() {
        return apicurioRegistryUrl;
    }

    @Override
    public String toString() {
        return "SchemaValidationConfig{"
               +
               "apicurioGlobalId="
               + apicurioGlobalId
               +
               ", apicurioRegistryUrl='"
               + apicurioRegistryUrl
               + '\''
               +
               '}';
    }

}
