/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.metadata.handler;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import java.util.stream.Collectors;

import io.kroxylicious.proxy.metadata.DescribeTopicLabelsResponse;
import io.kroxylicious.proxy.metadata.ListTopicsResponse;
import io.kroxylicious.proxy.metadata.selector.Selector;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface TopicMetadataSource {

    TopicMetadataSource EMPTY = new TopicMetadataSource() {
        @NonNull
        @Override
        public CompletionStage<DescribeTopicLabelsResponse> topicLabels(Collection<String> topicNames) {
            return CompletableFuture.completedStage(new DescribeTopicLabelsResponse(
                    topicNames.stream()
                            .collect(Collectors.toMap(Function.identity(), tn -> Map.of()))));
        }

        @NonNull
        @Override
        public CompletionStage<ListTopicsResponse> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors) {
            return CompletableFuture.completedStage(new ListTopicsResponse(
                    selectors.stream()
                            .collect(Collectors.toMap(Function.identity(), tn -> Set.of()))));
        }
    };

    /**
     * Gets the labels for a topic.
     * @param topicNames The names of the topics
     * @return a DescribeTopicLabelsResponse.
     * The returned DescribeTopicLabelsResponse is guaranteed to have a mapping for each of the given {@code topicNames}.
     */
    @NonNull
    CompletionStage<DescribeTopicLabelsResponse> topicLabels(Collection<String> topicNames);

    /**
     * Gets the names of all the topics matching the given {@code selector}.
     * @param selectors The selectors that topics need to match.
     * @return a ListTopicsResponse.
     * The returned ListTopicsResponse is guaranteed to have a mapping for each of the given {@code selectors}.
     */
    @NonNull
    CompletionStage<ListTopicsResponse> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors);

}
