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

    T visit(KeyPair keyPair);

    T visit(KeyStore keyStore);

    T visit(KeyPairSet keyPairSet);
}
