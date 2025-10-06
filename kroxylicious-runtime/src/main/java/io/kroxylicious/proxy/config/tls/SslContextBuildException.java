/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

public class SslContextBuildException extends RuntimeException {

    public SslContextBuildException(String message) {
        super(message);
    }

    public SslContextBuildException(Throwable cause) {
        super(cause);
    }

    public SslContextBuildException(String message, Throwable cause) {
        super(message, cause);
    }
}
