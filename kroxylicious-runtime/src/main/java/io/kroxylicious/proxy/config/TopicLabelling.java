/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.annotation.JsonCreator;

import io.kroxylicious.proxy.metadata.selector.Labels;

/**
 * Represents the labelling, with the given {@code labels}, of the topics which match any of the given requirements on topic names.
 * @param labels The labels
 * @param topicsNamed The names of topics to be labelled with {@code labels}.
 * @param topicsStartingWith The prefixes of names of topics to be labelled with {@code labels}.
 * @param topicsMatching Regular expressions matching the names of topics to be labelled with {@code labels}.
 */
public record TopicLabelling(Map<String, String> labels,
                             Set<String> topicsNamed,
                             Set<String> topicsStartingWith,
                             List<Pattern> topicsMatching) {
    public TopicLabelling {
        Objects.requireNonNull(labels);
        if (labels.isEmpty()) {
            throw new IllegalArgumentException("Invalid labels");
        }
        Labels.validate(labels);
    }

    @JsonCreator
    public TopicLabelling(Map<String, String> labels,
                          List<String> topicsNamed,
                          List<String> topicsStartingWith,
                          List<String> topicsMatching) {
        this(labels, new HashSet<>(topicsNamed), new HashSet<>(topicsStartingWith), topicsMatching.stream().map(Pattern::compile).toList());
    }

    public boolean matches(String topicName) {
        if (topicsNamed().contains(topicName)) {
            return true;
        }
        if (topicsStartingWith().stream().anyMatch(topicName::startsWith)) {
            return true;
        }
        if (topicsMatching().stream().anyMatch(pattern -> pattern.matcher(topicName).matches())) {
            return true;
        }
        return false;
    }
}
