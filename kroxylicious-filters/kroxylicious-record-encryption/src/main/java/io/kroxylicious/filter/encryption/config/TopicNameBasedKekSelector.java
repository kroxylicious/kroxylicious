/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.config;

import java.util.Set;
import java.util.concurrent.CompletionStage;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * KEK selection based on topic name
 * @param <K> the type of key
 */
public abstract class TopicNameBasedKekSelector<K> {

    /**
     * Returns a completion stage whose value, on successful completion, is a topic name selection containing the
     * resolved key ids for topics which could be resolved, and a set of unresolved topic names.
     * @param topicNames A set of topic names
     * @return A completion stage for the topic name selection
     */
    public abstract @NonNull CompletionStage<TopicNameKekSelection<K>> selectKek(@NonNull Set<String> topicNames);
}
