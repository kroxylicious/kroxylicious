/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;

import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class NettyTrustProvider implements TrustProvider {

    private final TrustProvider trustProvider;

    public NettyTrustProvider(TrustProvider trustProvider) {
        this.trustProvider = trustProvider;
    }

    public void apply(SslContextBuilder builder) {
        apply(new TrustManagerBuilder() {
            @Override
            public void trustCertCollectionFile(File file) {
                builder.trustManager(file);
            }

            @Override
            public void trustManager(TrustManagerFactory factory) {
                builder.trustManager(factory);
            }

            @Override
            public void insecure() {
                builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
            }
        });
    }

    @Override
    public void apply(TrustManagerBuilder builder) {
        trustProvider.apply(builder);
    }
}
