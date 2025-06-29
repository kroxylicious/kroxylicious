/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.util.Objects;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for validating a Bytebuffer holding a value.
 */
public class BytebufValidation {
    private final @Nullable SyntacticallyCorrectJsonConfig syntacticallyCorrectJsonConfig;
    private final @Nullable SchemaValidationConfig schemaValidationConfig;
    private final boolean allowNulls;
    private final boolean allowEmpty;

    /**
     * Create a new BytebufValidation
     * @param syntacticallyCorrectJsonConfig optional configuration, if non-null indicates ByteBuffer should contain syntactically correct JSON
     * @param schemaValidationConfig optional configuration, if non-null, indicates ByteBuffer to validate the data using the schema configuration
     * @param allowNulls whether a null byte-buffer should be considered valid
     * @param allowEmpty whether an empty byte-buffer should be considered valid
     */
    @JsonCreator
    public BytebufValidation(@JsonProperty("syntacticallyCorrectJson") @Nullable SyntacticallyCorrectJsonConfig syntacticallyCorrectJsonConfig,
                             @JsonProperty("schemaValidationConfig") @Nullable SchemaValidationConfig schemaValidationConfig,
                             @JsonProperty(value = "allowNulls", defaultValue = "true") @Nullable Boolean allowNulls,
                             @JsonProperty(value = "allowEmpty", defaultValue = "false") @Nullable Boolean allowEmpty) {
        this.syntacticallyCorrectJsonConfig = syntacticallyCorrectJsonConfig;
        this.schemaValidationConfig = schemaValidationConfig;
        this.allowNulls = allowNulls == null || allowNulls;
        this.allowEmpty = allowEmpty != null && allowEmpty;
    }

    /**
     * Get syntactically correct json config
     * @return optional containing syntacticallyCorrectJsonConfig if non-null, empty otherwise
     */
    public Optional<SyntacticallyCorrectJsonConfig> getSyntacticallyCorrectJsonConfig() {
        return Optional.ofNullable(syntacticallyCorrectJsonConfig);
    }

    /**
     * Get schema validation json config
     * @return optional containing schema validation config if non-null, empty otherwise
     */
    public Optional<SchemaValidationConfig> getSchemaValidationConfig() {
        return Optional.ofNullable(schemaValidationConfig);
    }

    /**
     * Are buffers valid if they are null on the {@link org.apache.kafka.common.record.Record}
     * @return allowNulls
     */
    public boolean isAllowNulls() {
        return allowNulls;
    }

    /**
     * Are buffers valid if they are empty (non-null, 0 length) on the {@link org.apache.kafka.common.record.Record}
     * @return allowEmpty
     */
    public boolean isAllowEmpty() {
        return allowEmpty;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BytebufValidation that = (BytebufValidation) o;
        return allowNulls == that.allowNulls && allowEmpty == that.allowEmpty && Objects.equals(syntacticallyCorrectJsonConfig,
                that.syntacticallyCorrectJsonConfig) && Objects.equals(schemaValidationConfig, that.schemaValidationConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(syntacticallyCorrectJsonConfig, schemaValidationConfig, allowNulls, allowEmpty);
    }

    @Override
    public String toString() {
        return "BytebufValidation{" +
                "syntacticallyCorrectJsonConfig=" + syntacticallyCorrectJsonConfig +
                ", schemaValidationConfig=" + schemaValidationConfig +
                ", allowNulls=" + allowNulls +
                ", allowEmpty=" + allowEmpty +
                '}';
    }
}
