/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.validation.config;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Configuration for Produce Request validation. Contains a description of the rules for validating
 * the data for all topic-partitions within a ProduceRequest.
 *
 * @param rules describes a list of rules, associating topics with some validation to be applied to produce data for that topic
 * @param defaultRule the default validation rule to be applied when no rule is matched for a topic within a ProduceRequest
 */
public record ValidationConfig(@JsonProperty("rules") @Nullable List<TopicMatchingRecordValidationRule> rules,
                               @JsonProperty("defaultRule") @Nullable RecordValidationRule defaultRule) {

    @Override
    public String toString() {
        return "ValidationConfig{" +
                "rules=" + rules +
                ", defaultRule=" + defaultRule +
                '}';
    }
}
