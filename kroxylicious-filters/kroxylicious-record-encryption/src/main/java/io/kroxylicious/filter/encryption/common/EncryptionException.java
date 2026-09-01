/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.filter.encryption.common;

import org.apache.kafka.common.errors.ApiException;

import edu.umd.cs.findbugs.annotations.NonNull;

import io.kroxylicious.kafka.common.InvalidRecordException;

/**
 * Exceptions to do with encryption.
 */
public class EncryptionException extends RuntimeException {
    /** The exception to be sent to the client. */
    @NonNull
    private final ApiException apiException;

    /**
     * Constructs an exception using an {@link InvalidRecordException} so that it is considered fatal by Kafka clients
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

    /**
     * Returns the exception to be sent to the client.
     * @return the exception to be sent to the client.
     */
    public ApiException getApiException() {
        return apiException;
    }
}
