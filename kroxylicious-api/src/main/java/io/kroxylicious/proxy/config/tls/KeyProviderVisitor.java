/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

/**
 * Visitor for implementations of {@link KeyProvider}
 * @param <T> result type of the visit
 */
public interface KeyProviderVisitor<T> {

    /**
     * Visits a {@link KeyPair}.
     * @param keyPair the key pair being visited
     * @return the result of the visit
     */
    T visit(KeyPair keyPair);

    /**
     * Visits a {@link KeyStore}.
     * @param keyStore the key store being visited
     * @return the result of the visit
     */
    T visit(KeyStore keyStore);

}
