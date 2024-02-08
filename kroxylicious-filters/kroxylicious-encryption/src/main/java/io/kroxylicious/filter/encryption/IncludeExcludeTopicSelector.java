/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.annotation.concurrent.ThreadSafe;

import edu.umd.cs.findbugs.annotations.NonNull;

@ThreadSafe
public class IncludeExcludeTopicSelector implements TopicSelector {

    private Map<String, Boolean> selected = new ConcurrentHashMap<>();

    // empty means include all
    private final Set<String> includeTopics = new HashSet<>();

    // empty means exclude none
    private final Set<String> excludeTopics = new HashSet<>();

    // empty means include all
    private final List<String> includePrefixes = new ArrayList<>();

    // empty means exclude none
    private final List<String> excludePrefixes = new ArrayList<>();

    private final boolean inclusionConfigured;
    private final boolean exclusionConfigured;

    public IncludeExcludeTopicSelector(@NonNull EnvelopeEncryption.TopicSelectorConfig config) {
        if (config.includeNames() != null) {
            includeTopics.addAll(config.includeNames());
        }
        if (config.excludeNames() != null) {
            excludeTopics.addAll(config.excludeNames());
        }
        if (config.includeNamePrefixes() != null) {
            includePrefixes.addAll(config.includeNamePrefixes());
        }
        if (config.excludeNamePrefixes() != null) {
            excludePrefixes.addAll(config.excludeNamePrefixes());
        }
        inclusionConfigured = !includePrefixes.isEmpty() || !includeTopics.isEmpty();
        exclusionConfigured = !excludePrefixes.isEmpty() || !excludeTopics.isEmpty();
    }

    @Override
    public boolean select(String topicName) {
        return selected.computeIfAbsent(topicName, this::isSelected);
    }

    private @NonNull Boolean isSelected(String topicName) {
        return !isExcluded(topicName) && isIncluded(topicName);
    }

    private boolean isIncluded(String topicName) {
        return !inclusionConfigured || (isIncludedByExactMatch(topicName) || isIncludedByPrefix(topicName));
    }

    private boolean isExcluded(String topicName) {
        return exclusionConfigured && (isExcludedByExactMatch(topicName) || isExcludedByPrefix(topicName));
    }

    private boolean isIncludedByExactMatch(String topicName) {
        return includeTopics.contains(topicName);
    }

    private boolean isIncludedByPrefix(String topicName) {
        return includePrefixes.stream().anyMatch(topicName::startsWith);
    }

    private boolean isExcludedByPrefix(String topicName) {
        return excludePrefixes.stream().anyMatch(topicName::startsWith);
    }

    private boolean isExcludedByExactMatch(String topicName) {
        return excludeTopics.contains(topicName);
    }

}
