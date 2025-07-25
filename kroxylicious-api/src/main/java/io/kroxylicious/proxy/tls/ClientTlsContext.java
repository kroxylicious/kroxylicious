/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.cert.X509Certificate;
import java.util.Optional;

import io.kroxylicious.proxy.filter.FilterContext;

/**
 * Exposes TLS information about the client-to-proxy connection to plugins, for example using {@link FilterContext#clientTlsContext()}.
 * This is implemented by the runtime for use by plugins.
 */
public interface ClientTlsContext {
    /**
     * @return The TLS server certificate that the proxy presented to the client during TLS handshake.
     */
    X509Certificate proxyServerCertificate();

    /**
     * @return the client's certificate, or empty if no TLS client certificate was presented during TLS handshake.
     */
    Optional<X509Certificate> clientCertificate();

}
