/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.systemtests.api.client;

import java.util.Objects;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A partition of a topic.
 *
 * @param topic topic name
 * @param partition partition index
 */
public record TopicPartition(@NonNull String topic, int partition) {

    public TopicPartition {
        Objects.requireNonNull(topic, "topic must not be null");
        if (partition < 0) {
            throw new IllegalArgumentException("partition must not be negative, was " + partition);
        }
    }
}
