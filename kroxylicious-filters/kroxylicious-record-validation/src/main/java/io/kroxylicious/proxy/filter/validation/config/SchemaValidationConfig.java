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
public record SchemaValidationConfig(URL apicurioRegistryUrl, long apicurioContentId) {
    /**
     * Construct SchemaValidationConfig
     * @param apicurioContentId apicurio registry version global identifier to be used for schema validation
     * @param apicurioRegistryUrl Apicurio Registry instance url
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "apicurioRegistryUrl", required = true) URL apicurioRegistryUrl,
                                  @JsonProperty(value = "apicurioContentId", required = true) long apicurioContentId) {
        this.apicurioContentId = apicurioContentId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
    }

    /**
     * @return the configured contentId to be used
     */
    @Override
    public long apicurioContentId() {
        return apicurioContentId;
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
        return "SchemaValidationConfig{" +
                "apicurioContentId=" + apicurioContentId +
                ", apicurioRegistryUrl='" + apicurioRegistryUrl + '\'' +
                '}';
    }

}
