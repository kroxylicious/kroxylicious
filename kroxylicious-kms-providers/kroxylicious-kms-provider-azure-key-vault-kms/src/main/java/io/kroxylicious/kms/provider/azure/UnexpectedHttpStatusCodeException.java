/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.net.http.HttpResponse;

// the response was unexpected
public class UnexpectedHttpStatusCodeException extends RuntimeException {
    private final int statusCode;

    public UnexpectedHttpStatusCodeException(HttpResponse<?> response) {
        super("response has an unexpected status code: " + response.statusCode() + " with body: " + response.body());
        this.statusCode = response.statusCode();
    }

    public UnexpectedHttpStatusCodeException(int code) {
        super("response has an unexpected status code: " + code);
        this.statusCode = code;
    }

    public int getStatusCode() {
        return statusCode;
    }
}
