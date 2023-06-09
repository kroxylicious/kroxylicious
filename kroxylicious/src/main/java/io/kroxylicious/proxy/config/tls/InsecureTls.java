/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public record InsecureTls(boolean insecure) implements TrustProvider {
    @Override
    public void apply(SslContextBuilder builder) {
        if (insecure) {
            builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
        }
    }
}
