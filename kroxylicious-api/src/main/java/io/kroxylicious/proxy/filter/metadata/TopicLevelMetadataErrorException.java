/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import io.kroxylicious.kafka.common.protocol.Errors;

/**
 * Indicates that there was an {@link Errors error} set on a {@link io.kroxylicious.kafka.common.message.MetadataResponseData.MetadataResponseTopic}.
 */
public class TopicLevelMetadataErrorException extends TopicNameMappingException {
    /**
     * Creates a new exception for the given error.
     * @param error the error set at the topic level of the metadata response
     */
    public TopicLevelMetadataErrorException(Errors error) {
        super(error);
    }
}
