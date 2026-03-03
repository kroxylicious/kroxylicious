/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import org.apache.kafka.common.protocol.Errors;

/**
 * Indicates that there was an {@link Error} set on a {@link org.apache.kafka.common.message.MetadataResponseData.MetadataResponseTopic}.
 */
public class TopicLevelMetadataErrorException extends TopicNameMappingException {
    public TopicLevelMetadataErrorException(Errors error) {
        super(error);
    }
}
