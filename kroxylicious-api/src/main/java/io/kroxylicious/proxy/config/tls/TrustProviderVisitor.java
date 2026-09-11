/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

/**
 * Visitor for implementations of {@link TrustProvider}
 * @param <T> result type of the visit
 */
public interface TrustProviderVisitor<T> {

    /**
     * Visits a {@link TrustStore}.
     * @param trustStore the trust store being visited
     * @return the result of the visit
     */
    T visit(TrustStore trustStore);

    /**
     * Visits an {@link InsecureTls}.
     * @param insecureTls the insecure TLS configuration being visited
     * @return the result of the visit
     */
    T visit(InsecureTls insecureTls);

    /**
     * Visits a {@link PlatformTrustProvider}.
     * @param platformTrustProviderTls the platform trust provider being visited
     * @return the result of the visit
     */
    T visit(PlatformTrustProvider platformTrustProviderTls);

}
