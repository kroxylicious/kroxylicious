/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 */
public record SchemaValidationConfig(String apicurioRegistryUrl, Long useApicurioGlobalId) {
    /**
     * Construct SyntacticallyCorrectJsonConfig
     * @param useApicurioGlobalId whether we expect the Object keys in the JSON to be unique
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "apicurioRegistryUrl") String apicurioRegistryUrl,
                                  @JsonProperty(value = "useApicurioGlobalId") Long useApicurioGlobalId) {
        this.useApicurioGlobalId = useApicurioGlobalId;
        this.apicurioRegistryUrl = apicurioRegistryUrl;
    }

    /**
     * @return the configured globalId to be used
     */
    @Override
    public Long useApicurioGlobalId() {
        return useApicurioGlobalId;
    }

    /**
     * @return the apicurio registry url to be used for this validation
     */
    @Override
    public String apicurioRegistryUrl() {
        return apicurioRegistryUrl;
    }

    @Override
    public String toString() {
        return "SchemaValidationConfig{" +
                "useApicurioGlobalId=" + useApicurioGlobalId +
                ", apicurioRegistryUrl='" + apicurioRegistryUrl + '\'' +
                '}';
    }

}
