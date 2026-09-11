/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

/**
 * The TLS client authentication modes.
 */
public enum TlsClientAuth {

    /**
     * Client authentication is required. The connection will fail if the client does not present a valid certificate.
     */
    REQUIRED,
    /**
     * Client authentication is requested. The client may decline to present a certificate.
     */
    REQUESTED,
    /**
     * Client authentication is not requested.
     */
    NONE;

}
