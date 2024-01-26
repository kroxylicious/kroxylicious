/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;

import io.netty.handler.ssl.JdkSslContext;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;

import io.kroxylicious.proxy.config.tls.NettyKeyProvider;
import io.kroxylicious.proxy.config.tls.NettyTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactoryContext;

public abstract class NettyFilterFactoryContext implements FilterFactoryContext {
    @Override
    public SSLContext clientSslContext(Tls tls) {
        SslContextBuilder builder = SslContextBuilder.forClient().sslProvider(SslProvider.JDK);
        if (tls.key() != null) {
            new NettyKeyProvider(tls.key()).apply(builder);
        }
        if (tls.trust() != null) {
            new NettyTrustProvider(tls.trust()).apply(builder);
        }
        try {
            SslContext build = builder.build();
            return ((JdkSslContext) build).context();
        }
        catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }
}
