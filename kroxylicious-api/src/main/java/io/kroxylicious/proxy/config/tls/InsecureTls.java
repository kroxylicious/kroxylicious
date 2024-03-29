/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

/**
 * A {@link TrustProvider} that allows the disabling trust verification.
 * <strong>Not recommended for production use.</strong>
 *
 * @param insecure true if trust is disabled.  If false, platform trust is used.
 */
public record InsecureTls(boolean insecure) implements TrustProvider {
    @Override
    public <T> T accept(TrustProviderVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
