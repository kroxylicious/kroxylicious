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

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

public class NettyTrustProvider {

    public static final String HTTPS_HOSTNAME_VERIFICATION = "HTTPS";
    private final TrustProvider trustProvider;

    public NettyTrustProvider(TrustProvider trustProvider) {
        this.trustProvider = trustProvider;
    }

    public SslContextBuilder apply(SslContextBuilder builder) {
        return trustProvider.accept(new TrustProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(TrustStore trustStore) {
                try {
                    enableHostnameVerification();
                    enableClientAuth(trustStore);
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

            private void enableClientAuth(TrustStore trustStore) {
                Optional.ofNullable(trustStore.clientAuth())
                        .map(NettyTrustProvider::toNettyClientAuth)
                        .ifPresent(builder::clientAuth);
            }

            @Override
            public SslContextBuilder visit(InsecureTls insecureTls) {
                try {
                    if (insecureTls.insecure()) {
                        disableHostnameVerification();
                        return builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                    }
                    else {
                        enableHostnameVerification();
                        return builder;
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for InsecureTls: " + insecureTls, e);
                }
            }

            @Override
            public SslContextBuilder visit(PlatformTrustProvider platformTrustProviderTls) {
                enableHostnameVerification();
                return builder;
            }

            private void enableHostnameVerification() {
                setEndpointAlgorithm(HTTPS_HOSTNAME_VERIFICATION);
            }

            private void disableHostnameVerification() {
                setEndpointAlgorithm(null);
            }

            private void setEndpointAlgorithm(@Nullable String httpsHostnameVerification) {
                builder.endpointIdentificationAlgorithm(httpsHostnameVerification);
            }
        });
    }

    @NonNull
    private static ClientAuth toNettyClientAuth(TlsClientAuth clientAuth) {
        return switch (clientAuth) {
            case REQUIRED -> ClientAuth.REQUIRE;
            case REQUESTED -> ClientAuth.OPTIONAL;
            case NONE -> ClientAuth.NONE;
        };
    }

}
