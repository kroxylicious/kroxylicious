/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import java.util.Map;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.protocol.Errors;

/**
 * The result of discovering the topic names for a collection of topic ids
 */
public interface TopicNameMapping {
    /**
     * @return true if there are any failures
     */
    boolean anyFailures();

    /**
     * @return map from topic id to successfully mapped topic name
     */
    Map<Uuid, String> topicNames();

    /**
     * @return map from topic id to kafka server error
     */
    Map<Uuid, Errors> failures();
}
