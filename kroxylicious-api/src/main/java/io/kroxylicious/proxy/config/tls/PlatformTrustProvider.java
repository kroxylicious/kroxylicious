/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

@SuppressWarnings("java:S6548")
public class PlatformTrustProvider implements TrustProvider {

    public static final PlatformTrustProvider INSTANCE = new PlatformTrustProvider();

    private PlatformTrustProvider() {
    }

    @Override
    public <T> T accept(TrustProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
