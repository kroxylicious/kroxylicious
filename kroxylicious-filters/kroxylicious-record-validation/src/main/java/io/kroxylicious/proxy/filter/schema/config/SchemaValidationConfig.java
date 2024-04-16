/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.schema.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for validating a component ByteBuffer of a ${@link org.apache.kafka.common.record.Record} is valid using the schema in Apicurio Registry.
 */
public class SchemaValidationConfig {
    private final Long useApicurioGlobalId;

    /**
     * Construct SyntacticallyCorrectJsonConfig
     * @param useApicurioGlobalId whether we expect the Object keys in the JSON to be unique
     */
    @JsonCreator
    public SchemaValidationConfig(@JsonProperty(value = "useApicurioGlobalId") Long useApicurioGlobalId) {
        this.useApicurioGlobalId = useApicurioGlobalId;
    }

    /**
     * Do we expect the Object keys in the JSON to be unique
     * @return the configured globalId to be used
     */
    public Long useApicurioGlobalId() {
        return useApicurioGlobalId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SchemaValidationConfig that = (SchemaValidationConfig) o;
        return Objects.equals(useApicurioGlobalId, that.useApicurioGlobalId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(useApicurioGlobalId);
    }

    @Override
    public String toString() {
        return "SchemaValidationConfig{" +
                "useApicurioGlobalId=" + useApicurioGlobalId +
                '}';
    }
}
