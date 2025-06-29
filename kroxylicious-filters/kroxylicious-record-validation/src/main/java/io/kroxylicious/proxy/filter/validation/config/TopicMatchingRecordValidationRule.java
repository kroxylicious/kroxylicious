/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.util.Objects;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration that links a topic name pattern with some record validation rules.
 */
public class TopicMatchingRecordValidationRule extends RecordValidationRule {

    private final Set<String> topicNames;

    /**
     * Construct a new TopicPatternRecordValidationRule
     * @param topicNames required topic names topics with these names are eligible to have this rule applied to them
     * @param keyRule optional validation to apply to Record's key
     * @param valueRule optional validation to apply to Record's value
     */
    @JsonCreator
    public TopicMatchingRecordValidationRule(@JsonProperty(value = "topicNames") @Nullable Set<String> topicNames,
                                             @JsonProperty(value = "keyRule") @Nullable BytebufValidation keyRule,
                                             @JsonProperty(value = "valueRule") @Nullable BytebufValidation valueRule) {
        super(keyRule, valueRule);
        this.topicNames = topicNames == null ? Set.of() : topicNames;
    }

    /**
     * Get topic name pattern that this rule is eligible to apply to
     * @return topic name pattern
     */
    public Set<String> getTopicNames() {
        return topicNames;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        TopicMatchingRecordValidationRule that = (TopicMatchingRecordValidationRule) o;
        return Objects.equals(topicNames, that.topicNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), topicNames);
    }

    @Override
    public String toString() {
        return "TopicMatchingRecordValidationRule{" +
                "topicNames=" + topicNames +
                ", keyRule=" + keyRule +
                ", valueRule=" + valueRule +
                '}';
    }
}
