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

    T visit(TrustStore trustStore);

    T visit(InsecureTls insecureTls);

    T visit(PlatformTrustProvider platformTrustProviderTls);

}
