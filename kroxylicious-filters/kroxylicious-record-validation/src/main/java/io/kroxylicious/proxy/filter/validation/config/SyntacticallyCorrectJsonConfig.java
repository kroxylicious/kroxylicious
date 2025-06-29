/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for validating a component ByteBuffer of a {@link org.apache.kafka.common.record.Record} is syntactically correct JSON.
 */
public class SyntacticallyCorrectJsonConfig {
    private final boolean validateObjectKeysUnique;

    /**
     * Construct SyntacticallyCorrectJsonConfig
     * @param validateObjectKeysUnique whether we expect the Object keys in the JSON to be unique
     */
    @JsonCreator
    public SyntacticallyCorrectJsonConfig(@JsonProperty(value = "validateObjectKeysUnique", defaultValue = "false") @Nullable Boolean validateObjectKeysUnique) {
        this.validateObjectKeysUnique = validateObjectKeysUnique != null && validateObjectKeysUnique;
    }

    /**
     * Do we expect the Object keys in the JSON to be unique
     * @return true if we want to validate that the Object keys to be unique
     */
    public boolean isValidateObjectKeysUnique() {
        return validateObjectKeysUnique;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SyntacticallyCorrectJsonConfig that = (SyntacticallyCorrectJsonConfig) o;
        return validateObjectKeysUnique == that.validateObjectKeysUnique;
    }

    @Override
    public int hashCode() {
        return Objects.hash(validateObjectKeysUnique);
    }

    @Override
    public String toString() {
        return "SyntacticallyCorrectJsonConfig{" +
                "validateObjectKeysUnique=" + validateObjectKeysUnique +
                '}';
    }
}
