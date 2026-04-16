/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.security.KeyStore;

/**
 * Provides TLS key material (private key and certificate chain) as a Java {@link KeyStore}.
 * Implementations are plugin instances referenced by name from consumer configs.
 */
public interface KeyMaterialProvider {

    /**
     * Returns a {@link KeyStore} containing the private key and certificate chain.
     *
     * @return the key store
     */
    KeyStore keyStore();
}
