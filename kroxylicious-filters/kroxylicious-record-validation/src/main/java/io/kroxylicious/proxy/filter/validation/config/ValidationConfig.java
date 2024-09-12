/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for Produce Request validation. Contains a description of the rules for validating
 * the data for all topic-partitions with a ProduceRequest and how to handle partial failures (where
 * some topic-partitions are valid and others are invalid within a single ProduceRequest)
 */
public class ValidationConfig {

    /**
     * If this is enabled then the proxy will (for non-transactional requests):
     * 1. filter out invalid topic-partitions from the ProduceRequest
     * 2. forward the filtered request up the chain
     * 3. intercept the produce response and augment in failure details for the invalid topic-partitions
     */
    private final boolean forwardPartialRequests;
    private final List<TopicMatchingRecordValidationRule> rules;
    private final RecordValidationRule defaultRule;

    /**
     * Construct a new ValidationConfig
     * @param forwardPartialRequests describes whether partial ProduceRequest data should be forwarded to the broker (for non-transactional requests)
     * @param rules describes a list of rules, associating topics with some validation to be applied to produce data for that topic
     * @param defaultRule the default validation rule to be applied when no rule is matched for a topic within a ProduceRequest
     */
    @JsonCreator
    public ValidationConfig(
            @JsonProperty(value = "forwardPartialRequests", defaultValue = "false")
            Boolean forwardPartialRequests,
            @JsonProperty("rules")
            List<TopicMatchingRecordValidationRule> rules,
            @JsonProperty("defaultRule")
            RecordValidationRule defaultRule
    ) {
        this.forwardPartialRequests = forwardPartialRequests != null && forwardPartialRequests;
        this.rules = rules;
        this.defaultRule = defaultRule;
    }

    /**
     * Get the rules
     * @return rules
     */
    public List<TopicMatchingRecordValidationRule> getRules() {
        return rules;
    }

    /**
     * is forwarding partial requests enabled?
     * @return true if forwarding partial requests enabled
     */
    public boolean isForwardPartialRequests() {
        return forwardPartialRequests;
    }

    /**
     * get default rule
     * @return default rule (not null)
     */
    public RecordValidationRule getDefaultRule() {
        return defaultRule;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ValidationConfig that = (ValidationConfig) o;
        return forwardPartialRequests == that.forwardPartialRequests
               && Objects.equals(rules, that.rules)
               && Objects.equals(
                       defaultRule,
                       that.defaultRule
               );
    }

    @Override
    public int hashCode() {
        return Objects.hash(forwardPartialRequests, rules, defaultRule);
    }

    @Override
    public String toString() {
        return "ValidationConfig{"
               +
               "forwardPartialRequests="
               + forwardPartialRequests
               +
               ", rules="
               + rules
               +
               ", defaultRule="
               + defaultRule
               +
               '}';
    }
}
