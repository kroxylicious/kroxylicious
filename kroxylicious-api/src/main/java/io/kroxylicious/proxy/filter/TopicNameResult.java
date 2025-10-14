/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * The result of an attempt to retrieve a topic name for a topic id.
 * @param topicName name of topic if discovered
 * @param exception exception if topic name not discovered
 */
public record TopicNameResult(@Nullable String topicName, @Nullable TopicNameLookupException exception) {
    public TopicNameResult {
        if (topicName != null && exception != null) {
            throw new IllegalArgumentException("only one of topicName and exception should be non-null");
        }
        if (topicName == null && exception == null) {
            throw new IllegalArgumentException("one of topicName and exception should be non-null, both are null");
        }
    }

    public static TopicNameResult forName(String topicName) {
        return new TopicNameResult(topicName, null);
    }

    public static TopicNameResult forException(TopicNameLookupException exception) {
        return new TopicNameResult(null, exception);
    }
}
