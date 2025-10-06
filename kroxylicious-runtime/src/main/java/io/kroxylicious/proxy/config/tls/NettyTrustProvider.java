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
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import io.kroxylicious.proxy.config.secret.PasswordProvider;

import edu.umd.cs.findbugs.annotations.Nullable;

public class NettyTrustProvider {

    public static final String HTTPS_HOSTNAME_VERIFICATION = "HTTPS";
    private final TrustProvider trustProvider;

    public NettyTrustProvider(TrustProvider trustProvider) {
        this.trustProvider = trustProvider;
    }

    private static ClientAuth toNettyClientAuth(TlsClientAuth clientAuth) {
        return switch (clientAuth) {
            case REQUIRED -> ClientAuth.REQUIRE;
            case REQUESTED -> ClientAuth.OPTIONAL;
            case NONE -> ClientAuth.NONE;
        };
    }

    public SslContextBuilder apply(SslContextBuilder builder) {
        return trustProvider.accept(new TrustProviderVisitor<>() {
            @Override
            public SslContextBuilder visit(TrustStore trustStore) {
                try {
                    enableHostnameVerification();
                    TlsClientAuth clientAuthMode = enableClientAuth(trustStore);
                    if (trustStore.isPemType()) {
                        if (clientAuthMode == TlsClientAuth.REQUESTED) {
                            throw new SslContextBuildException(
                                    "REQUESTED client authentication mode is not supported for PEM trust stores. " +
                                            "PEM files cannot be wrapped to provide proper certificate validation when certificates are presented. " +
                                            "Please use JKS or PKCS12 format, or use REQUIRED/NONE client authentication modes instead.");
                        }
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

                            // For REQUESTED mode, wrap the trust manager to validate certificates if presented
                            if (clientAuthMode == TlsClientAuth.REQUESTED) {
                                TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
                                if (trustManagers != null && trustManagers.length > 0 && trustManagers[0] instanceof X509TrustManager) {
                                    return builder.trustManager(
                                            new RequestedClientAuthTrustManager((X509TrustManager) trustManagers[0])
                                    );
                                }
                            }

                            return builder.trustManager(trustManagerFactory);
                        }
                    }
                }
                catch (Exception e) {
                    throw new SslContextBuildException("Error building SSLContext for TrustStore: " + trustStore, e);
                }
            }

            private TlsClientAuth enableClientAuth(TrustStore trustStore) {
                TlsClientAuth tlsClientAuth = Optional.ofNullable(trustStore.trustOptions())
                        .filter(ServerOptions.class::isInstance)
                        .map(ServerOptions.class::cast)
                        .map(ServerOptions::clientAuth)
                        .orElse(TlsClientAuth.REQUIRED);
                ClientAuth clientAuth = toNettyClientAuth(tlsClientAuth);
                builder.clientAuth(clientAuth);
                return tlsClientAuth;
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

}
