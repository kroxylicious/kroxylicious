/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

/**
 * Thrown when TLS credentials are invalid — for example, when a private key does not match
 * the certificate, or when the certificate chain is structurally incorrect.
 */
public class BadTlsCredentialsException extends RuntimeException {

    public BadTlsCredentialsException(String message) {
        super(message);
    }

    public BadTlsCredentialsException(String message, Throwable cause) {
        super(message, cause);
    }
}
