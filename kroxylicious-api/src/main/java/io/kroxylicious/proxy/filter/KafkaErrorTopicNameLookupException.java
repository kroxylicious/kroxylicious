/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.filter;

import org.apache.kafka.common.protocol.Errors;

/**
 * Indicates that a metadata request for a topic resulted in an error response
 */
public class KafkaErrorTopicNameLookupException extends TopicNameLookupException {
    private final Errors error;

    public KafkaErrorTopicNameLookupException(Errors error, String message) {
        super(message);
        this.error = error;
    }

    public Errors getError() {
        return error;
    }
}
