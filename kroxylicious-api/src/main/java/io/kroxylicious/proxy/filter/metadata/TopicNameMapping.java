/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

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
     * @return immutable map from topic id to successfully mapped topic name
     */
    Map<Uuid, String> topicNames();

    /**
     * Describes the reason for every failed mapping. Expected exception types are:
     * <ul>
     *     <li>{@link TopLevelMetadataErrorException} indicates that we attempted to obtain Metadata from upstream, but received a top-level error in the response</li>
     *     <li>{@link TopicLevelMetadataErrorException} indicates that we attempted to obtain Metadata from upstream, but received a topic-level error in the response</li>
     *     <li>{@link TopicNameMappingException} can be used to convey any other exception</li>
     * </ul>
     * All the exception types offer {@link TopicNameMappingException#getError()} for conveniently determining the cause. Unhandled
     * exceptions will be mapped to an {@link Errors#UNKNOWN_SERVER_ERROR}. Callers will be able to use this to detect expected
     * cases like {@link Errors#UNKNOWN_TOPIC_ID}.
     * @return immutable map from topic id to kafka server error
     */
    Map<Uuid, TopicNameMappingException> failures();
}
