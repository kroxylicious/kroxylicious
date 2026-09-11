/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter.metadata;

import io.kroxylicious.kafka.common.protocol.Errors;

/**
 * Indicates that there was an {@link Errors error} set at the top level of {@link io.kroxylicious.kafka.common.message.MetadataResponseData}.
 */
public class TopLevelMetadataErrorException extends TopicNameMappingException {
    /**
     * Creates a new exception for the given error.
     * @param error the error set at the top level of the metadata response
     */
    public TopLevelMetadataErrorException(Errors error) {
        super(error);
    }
}
