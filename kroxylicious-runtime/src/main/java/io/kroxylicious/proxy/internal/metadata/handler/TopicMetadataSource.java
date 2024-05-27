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

import io.kroxylicious.proxy.metadata.selector.Selector;

import edu.umd.cs.findbugs.annotations.NonNull;

public interface TopicMetadataSource {

    TopicMetadataSource EMPTY = new TopicMetadataSource() {
        @NonNull
        @Override
        public CompletionStage<Map<String, Map<String, String>>> topicLabels(Collection<String> topicNames) {
            return CompletableFuture.completedStage(Map.of());
        }

        @NonNull
        @Override
        public CompletionStage<Map<Selector, Set<String>>> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors) {
            return CompletableFuture.completedStage(Map.of());
        }
    };

    /**
     * Gets the labels for a topic.
     * @param topicNames The names of the topics
     * @return an immutable from topic name to labels for that topic (which may be empty), or null if that topic is not known.
     */
    // TODO harden up the semantics of "not known": Does this need to be reconciled with up-to-date metadata, (should it make a metadata request)?
    @NonNull
    CompletionStage<Map<String, Map<String, String>>> topicLabels(Collection<String> topicNames);

    /**
     * Gets the names of all the topics matching the given {@code selector}.
     * @param selectors The selectors that topics need to match.
     * @return an immutable map from the selector to all the topics which match that selector.
     */
    // TODO harden up whether this is allowed to return the names of topics which don't actually exist in the cluster?
    // this is relevant for the implementation e.g. taking a static labelling as config, but having to reconcile that with the
    // extant topics (or not). If we use a cache is there a bound on how stale it can be?
    @NonNull
    CompletionStage<Map<Selector, Set<String>>> topicsMatching(Collection<String> topicNames, Collection<Selector> selectors);

}
