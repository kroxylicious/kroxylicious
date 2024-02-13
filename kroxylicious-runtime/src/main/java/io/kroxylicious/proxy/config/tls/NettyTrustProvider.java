/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.File;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Optional;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

public class NettyTrustProvider {

    private final TrustProvider trustProvider;

    public NettyTrustProvider(TrustProvider trustProvider) {
        this.trustProvider = trustProvider;
    }

    public SslContextBuilder apply(SslContextBuilder builder) {
        return trustProvider.accept(new TrustProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(TrustStore trustStore) {
                try {
                    if (trustStore.isPemType()) {
                        return builder.trustManager(new File(trustStore.storeFile()));
                    }
                    else {
                        try (var is = new FileInputStream(trustStore.storeFile())) {
                            var password = Optional.ofNullable(trustStore.storePasswordProvider()).map(PasswordProvider::getProvidedPassword).map(String::toCharArray)
                                    .orElse(null);
                            var keyStore = KeyStore.getInstance(trustStore.getType());
                            keyStore.load(is, password);

                            var trustManagerFactory = TrustManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                            trustManagerFactory.init(keyStore);
                            return builder.trustManager(trustManagerFactory);
                        }
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for TrustStore: " + trustStore, e);
                }
            }

            @Override
            public SslContextBuilder visit(InsecureTls insecureTls) {
                try {
                    if (insecureTls.insecure()) {
                        return builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                    }
                    else {
                        return builder;
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for InsecureTls: " + insecureTls, e);
                }
            }
        });
    }

}
