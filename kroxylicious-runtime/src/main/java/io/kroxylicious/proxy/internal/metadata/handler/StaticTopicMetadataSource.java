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
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.config.TopicLabelling;
import io.kroxylicious.proxy.metadata.selector.Selector;

import edu.umd.cs.findbugs.annotations.NonNull;

public class StaticTopicMetadataSource implements TopicMetadataSource {
    private final @NonNull List<TopicLabelling> labellings;

    private final ConcurrentHashMap<String, Map<String, String>> topicToLabels;

    public StaticTopicMetadataSource(@NonNull List<TopicLabelling> labellings) {
        // 1. Find labellings that intersect (e.g. foo=x and foo=y)
        record SingleLabel(String labelKey, String labelValue, TopicLabelling labelling) {}

        var byLabelKey = labellings.stream()
                .flatMap(labelling -> labelling.labels().entrySet().stream()
                        .map(entry -> new SingleLabel(entry.getKey(), entry.getValue(), labelling)))
                .collect(Collectors.groupingBy(SingleLabel::labelKey));
        byLabelKey.forEach((labelKey, singleLabels) -> {
            // collect together the labelling that have the same label value (e.g. all the x's and all the y's)
            var byLabelValue = singleLabels.stream().collect(Collectors.groupingBy(SingleLabel::labelValue));
            for (var entry : byLabelValue.entrySet()) {
                var labelValue1 = entry.getKey();
                var labelingsForLabelValue1 = entry.getValue();
                for (var y : byLabelValue.entrySet()) {
                    var labelValue2 = y.getKey();
                    if (labelValue1.equals(labelValue2)) {
                        continue;
                    }
                    var labelingsForLabelValue2 = y.getValue();
                    for (var labelling1 : labelingsForLabelValue1) {
                        for (var labelling2 : labelingsForLabelValue2) {
                            if (labelling1.labelling().maybeSomeTopicsInCommon(labelling2.labelling())) {
                                throw new IllegalArgumentException("A topic cannot be labelled with both "
                                        + labelKey + "=" + labelValue1 + " and "
                                        + labelKey + "=" + labelValue2 + ": "
                                        + labelling1.labelling() + " could have a topic in common with " + labelling2.labelling());
                            }
                        }
                    }
                }
            }
        });

        this.labellings = Objects.requireNonNull(labellings);
        this.topicToLabels = new ConcurrentHashMap<>();
    }

    @NonNull
    @Override
    public CompletionStage<Map<String, Map<String, String>>> topicLabels(Collection<String> topicNames) {
        Map<String, Map<String, String>> result = new HashMap<>();
        for (String topicName : topicNames) {
            Map<String, String> topicLabels = maybeComputeTopicLabels(topicName);
            result.put(topicName, topicLabels);
        }
        return CompletableFuture.completedStage(result);
    }

    @NonNull
    private Map<String, String> maybeComputeTopicLabels(String topicName) {
        return topicToLabels.computeIfAbsent(topicName, k -> {
            Map<String, String> topicLabels = new HashMap<>();
            for (var labelling : labellings) {
                if (labelling.matches(topicName)) {
                    topicLabels.putAll(labelling.labels());
                }
            }
            return topicLabels;
        });
    }

    @NonNull
    @Override
    public CompletionStage<Map<Selector, Set<String>>> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors) {
        Map<Selector, Set<String>> result = new HashMap<>();
        for (String topicName : topicNames) {
            Map<String, String> topicLabels = maybeComputeTopicLabels(topicName);
            for (var selector : selectors) {
                Set<String> matches = result.computeIfAbsent(selector, k -> new HashSet<>());
                if (selector.test(topicLabels)) {
                    matches.add(topicName);
                }
            }
        }
        return CompletableFuture.completedStage(result);
    }

}
