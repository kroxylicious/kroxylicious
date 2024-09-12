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

/**
 * Describes an optional key validation rule and an optional value validation rule which will
 * be applied to a {@link org.apache.kafka.common.record.Record} to validate its contents.
 */
public class RecordValidationRule {

    /**
     * rule to apply to record key
     */
    protected final BytebufValidation keyRule;

    /**
     * rule to apply to record value
     */
    protected final BytebufValidation valueRule;

    /**
     * Construct new RecordValidationRule
     * @param keyRule optional validation to apply to Record's key
     * @param valueRule optional validation to apply to Record's value
     */
    @JsonCreator
    public RecordValidationRule(
            @JsonProperty(value = "keyRule")
            BytebufValidation keyRule,
            @JsonProperty(value = "valueRule")
            BytebufValidation valueRule
    ) {
        this.keyRule = keyRule;
        this.valueRule = valueRule;
    }

    /**
     * get optional key rule
     * @return optional containing key rule if non-null, else empty optional
     */
    public Optional<BytebufValidation> getKeyRule() {
        return Optional.ofNullable(keyRule);
    }

    /**
     * get value rule
     * @return optional containing value rule if non-null, else empty optional
     */
    public Optional<BytebufValidation> getValueRule() {
        return Optional.ofNullable(valueRule);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordValidationRule that = (RecordValidationRule) o;
        return Objects.equals(keyRule, that.keyRule) && Objects.equals(valueRule, that.valueRule);
    }

    @Override
    public int hashCode() {
        return Objects.hash(keyRule, valueRule);
    }

    @Override
    public String toString() {
        return "RecordValidationRule{"
               +
               "keyRule="
               + keyRule
               +
               ", valueRule="
               + valueRule
               +
               '}';
    }
}
