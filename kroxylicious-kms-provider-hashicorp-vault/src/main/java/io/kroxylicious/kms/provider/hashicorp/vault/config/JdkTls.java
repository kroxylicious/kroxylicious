/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault.config;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.config.tls.TrustProviderVisitor;
import io.kroxylicious.proxy.config.tls.TrustStore;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Builds a JDK SSLContext used for vault communicateion.
 *
 * @param tls tls configuration
 *
 * TODO ability to restrict by TLS protocol and cipher suite.
 */
public record JdkTls(Tls tls) {

    private static final Logger logger = LoggerFactory.getLogger(JdkTls.class);

    public JdkTls {
        if (tls != null && tls.key() != null) {
            logger.warn("TLS key material is currently not supported by the vault client");
        }
    }

    public static final X509TrustManager INSECURE_TRUST_MANAGER = new X509TrustManager() {

        // suppressing sonar security warning as we are intentionally throwing security out the window
        @SuppressWarnings("java:S4830")
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // do nothing, the api is to throw if not trusted
        }

        // suppressing sonar security warning as we are intentionally throwing security out the window
        @SuppressWarnings("java:S4830")
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // do nothing, the api is to throw if not trusted
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    };
    public static final TrustManager[] INSECURE_TRUST_MANAGERS = { INSECURE_TRUST_MANAGER };

    public SSLContext sslContext() {
        try {
            if (tls != null && tls.trust() != null) {
                return customSSLContext(tls.trust());
            }
            else {
                return SSLContext.getDefault();
            }
        }
        catch (Exception e) {
            throw new SslConfigurationException(e);
        }
    }

    @NonNull
    private SSLContext customSSLContext(TrustProvider trust) {
        TrustManager[] trustManagers = getTrustManagers(trust);
        return getSslContext(trustManagers);
    }

    @NonNull
    private static SSLContext getSslContext(TrustManager[] trustManagers) {
        try {
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, trustManagers, new SecureRandom());
            return context;
        }
        catch (Exception e) {
            throw new SslConfigurationException(e);
        }
    }

    /* exposed for testing */
    static TrustManager[] getTrustManagers(TrustProvider trust) {

        return trust.accept(new TrustProviderVisitor<>() {
            @Override
            public TrustManager[] visit(TrustStore trustStore) {
                if (trustStore.isPemType()) {
                    throw new SslConfigurationException("PEM trust not supported by vault yet");
                }
                try {
                    KeyStore instance = KeyStore.getInstance(trustStore.getType());
                    char[] charArray = trustStore.storePasswordProvider() != null ? trustStore.storePasswordProvider().getProvidedPassword().toCharArray() : null;
                    instance.load(new FileInputStream(trustStore.storeFile()), charArray);
                    TrustManagerFactory managerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    managerFactory.init(instance);
                    return managerFactory.getTrustManagers();
                }
                catch (Exception e) {
                    throw new SslConfigurationException(e);
                }
            }

            @Override
            public TrustManager[] visit(InsecureTls insecureTls) {
                if (insecureTls.insecure()) {
                    return INSECURE_TRUST_MANAGERS;
                }
                else {
                    return getDefaultTrustManagers();
                }
            }

            private static TrustManager[] getDefaultTrustManagers() {
                try {
                    TrustManagerFactory factory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                    factory.init((KeyStore) null);
                    return factory.getTrustManagers();
                }
                catch (Exception e) {
                    throw new SslConfigurationException(e);
                }
            }
        });
    }
}
