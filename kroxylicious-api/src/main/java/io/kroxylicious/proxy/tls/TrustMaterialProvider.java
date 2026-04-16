/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.KeyStore;

/**
 * Provides TLS trust material (trusted CA certificates) as a Java {@link KeyStore}.
 * Implementations are plugin instances referenced by name from consumer configs.
 */
public interface TrustMaterialProvider {

    /**
     * Returns a {@link KeyStore} containing the trusted certificates.
     *
     * @return the trust store
     */
    KeyStore trustStore();
}
