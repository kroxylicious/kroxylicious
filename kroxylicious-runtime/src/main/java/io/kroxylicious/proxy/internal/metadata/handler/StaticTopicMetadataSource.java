/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata.handler;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import io.kroxylicious.proxy.config.TopicLabelling;
import io.kroxylicious.proxy.metadata.selector.Selector;

import edu.umd.cs.findbugs.annotations.NonNull;

public class StaticTopicMetadataSource implements TopicMetadataSource {
    private final @NonNull List<TopicLabelling> labellings;

    public StaticTopicMetadataSource(@NonNull List<TopicLabelling> labellings) {
        this.labellings = Objects.requireNonNull(labellings);
        // TODO the tricky thing here is we don't know the set of topics which exists in the cluster,
        // but because the TopicLabelling selects topics using prefixes and regex we need that in order to compute topic's labels.
        // And that changes during runtime, even in the absence of an API for mutating a topic's labels, as topics are created and deleted
    }

    @NonNull
    @Override
    public CompletionStage<Map<String, Map<String, String>>> topicLabels(Collection<String> topicNames) {
        Map<String, Map<String, String>> result = new HashMap<>();

        // TreeMap<String, TreeMap<String, Set<String>>> keysToValuesToTopics = new TreeMap<>();
        for (String topicName : topicNames) {
            Map<String, String> topicLabels = new HashMap<>();
            for (var labelling : labellings) {
                if (labelling.topicsNamed().contains(topicName)
                        || labelling.topicsStartingWith().stream().anyMatch(topicName::startsWith)
                        || labelling.topicsMatching().stream().anyMatch(pattern -> pattern.matcher(topicName).matches())) {
                    topicLabels.putAll(labelling.labels());
                }
            }
            result.put(topicName, topicLabels);
        }
        return CompletableFuture.completedStage(result);
        // var resultMap = new HashMap<String, Map<String, String>>(topicNames.size());
        // for (var name : topicNames) {
        // resultMap.put(name, topicToLabels.get(name));
        // }
        // return CompletableFuture.completedStage(Collections.unmodifiableMap(resultMap));
    }

    @NonNull
    @Override
    public CompletionStage<Map<Selector, Set<String>>> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors) {
        Map<Selector, Set<String>> result = new HashMap<>();
        for (String topicName : topicNames) {
            Map<String, String> topicLabels = new HashMap<>();
            for (var labelling : labellings) {
                if (labelling.topicsNamed().contains(topicName)
                        || labelling.topicsStartingWith().stream().anyMatch(topicName::startsWith)
                        || labelling.topicsMatching().stream().anyMatch(pattern -> pattern.matcher(topicName).matches())) {
                    topicLabels.putAll(labelling.labels());
                }
            }
            for (var selector : selectors) {
                if (selector.test(topicLabels)) {
                    result.computeIfAbsent(selector, k -> new HashSet<>()).add(topicName);
                }
            }
        }
        return CompletableFuture.completedStage(result);
    }

}
