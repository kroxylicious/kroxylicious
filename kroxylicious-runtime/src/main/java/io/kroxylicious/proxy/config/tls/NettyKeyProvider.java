/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.KeyManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;

public class NettyKeyProvider implements KeyProvider {

    private final KeyProvider delegate;

    public NettyKeyProvider(KeyProvider delegate) {
        this.delegate = delegate;
    }

    public SslContextBuilder forClient() {
        SslContextBuilder builder = SslContextBuilder.forClient();
        return apply(builder);
    }

    public SslContextBuilder apply(SslContextBuilder builder) {
        apply(new KeyManagerBuilder() {
            @Override
            public void keyManager(File keyCertChainFile, File keyFile, String keyPassword) {
                builder.keyManager(keyCertChainFile, keyFile, keyPassword);
            }

            @Override
            public void keyManager(KeyManagerFactory factory) {
                builder.keyManager(factory);
            }
        });
        return builder;
    }

    public SslContextBuilder forServer() {
        AtomicReference<SslContextBuilder> builder = new AtomicReference<>();
        apply(new KeyManagerBuilder() {
            @Override
            public void keyManager(File keyCertChainFile, File keyFile, String keyPassword) {
                builder.set(SslContextBuilder.forServer(keyCertChainFile, keyFile, keyPassword));
            }

            @Override
            public void keyManager(KeyManagerFactory factory) {
                builder.set(SslContextBuilder.forServer(factory));
            }
        });
        return builder.get();
    }

    @Override
    public void apply(KeyManagerBuilder builder) {
        this.delegate.apply(builder);
    }
}
