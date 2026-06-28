/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import org.apache.kafka.common.protocol.Errors;

/**
 * Indicates that there was an {@link Error} set at the top level of {@link org.apache.kafka.common.message.MetadataResponseData}.
 */
public class TopLevelMetadataErrorException extends TopicNameMappingException {
    public TopLevelMetadataErrorException(Errors error) {
        super(error);
    }
}
