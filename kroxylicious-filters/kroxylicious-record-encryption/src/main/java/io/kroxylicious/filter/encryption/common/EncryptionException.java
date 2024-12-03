/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.ApiException;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Exceptions to do with encryption.
 */
public class EncryptionException extends RuntimeException {
    @NonNull
    private final ApiException apiException;

    /**
     * Constructs an exception using an {@see InvalidRecordException} so that it is considered fatal by Kafka clients
     * @param message to be included in both the logs and the client response (where messages are included by the protocol)
     */
    public EncryptionException(String message) {
        this(message, new InvalidRecordException(message));
    }

    /**
     * Constructs an encryption exception
     * @param message the message for <em>*this*</em> exception.
     * @param apiException the Exception to be sent to the client.
     */
    public EncryptionException(String message, @NonNull ApiException apiException) {
        super(message);
        this.apiException = apiException;
    }

    public ApiException getApiException() {
        return apiException;
    }
}
