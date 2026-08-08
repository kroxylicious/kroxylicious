/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.net.http.HttpResponse;

/**
 * Thrown when an HTTP response from Azure has an unexpected status code.
 */
public class UnexpectedHttpStatusCodeException extends RuntimeException {
    /**
     * The unexpected HTTP status code.
     * @serial
     */
    private final int statusCode;

    /**
     * Creates the exception from an HTTP response.
     *
     * @param response the response with the unexpected status code.
     */
    public UnexpectedHttpStatusCodeException(HttpResponse<?> response) {
        super("response has an unexpected status code: " + response.statusCode() + " with body: " + response.body());
        this.statusCode = response.statusCode();
    }

    /**
     * Creates the exception from an HTTP status code.
     *
     * @param code the unexpected status code.
     */
    public UnexpectedHttpStatusCodeException(int code) {
        super("response has an unexpected status code: " + code);
        this.statusCode = code;
    }

    /**
     * The unexpected HTTP status code.
     *
     * @return the status code.
     */
    public int getStatusCode() {
        return statusCode;
    }
}
