/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

public class PlatformTrustProvider implements TrustProvider {

    @Override
    public <T> T accept(TrustProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
