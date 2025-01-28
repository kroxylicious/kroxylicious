/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.Optional;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
import io.kroxylicious.proxy.config.tls.KeyPairSet;
import io.kroxylicious.proxy.config.tls.KeyProvider;
import io.kroxylicious.proxy.config.tls.KeyProviderVisitor;
import io.kroxylicious.proxy.config.tls.PlatformTrustProvider;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.config.tls.TrustProvider;
import io.kroxylicious.proxy.config.tls.TrustProviderVisitor;
import io.kroxylicious.proxy.config.tls.TrustStore;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Encapsulates parameters for an TLS connection with AWS.
 *
 * @param tls tls configuration
 *
 * TODO ability to restrict by TLS protocol and cipher suite (<a href="https://github.com/kroxylicious/kroxylicious/issues/1006">#1006</a>)
 */
public record JdkTls(Tls tls) {

    private static final Logger logger = LoggerFactory.getLogger(JdkTls.class);

    public JdkTls {
        if (tls != null && tls.key() != null) {
            logger.warn("TLS key material is currently not supported by this client");
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
            if (tls != null) {
                TrustManager[] trustManagers = null;
                KeyManager[] keyManagers = null;
                if (tls.trust() != null) {
                    trustManagers = getTrustManagers(tls.trust());
                }
                if (tls.key() != null) {
                    keyManagers = getKeyManagers(tls.key());
                }
                return getSslContext(trustManagers, keyManagers);
            }
            else {
                return SSLContext.getDefault();
            }
        }
        catch (Exception e) {
            throw new SslConfigurationException(e);
        }
    }

    @VisibleForTesting
    static KeyManager[] getKeyManagers(KeyProvider key) {
        return key.accept(new KeyProviderVisitor<>() {
            @Override
            public KeyManager[] visit(KeyPair keyPair) {
                throw new SslConfigurationException("KeyPair is not supported by vault KMS yet");
            }

            @Override
            public KeyManager[] visit(io.kroxylicious.proxy.config.tls.KeyStore keyStore) {
                try {
                    if (keyStore.isPemType()) {
                        throw new SslConfigurationException("PEM is not supported by this client");
                    }
                    KeyStore store = KeyStore.getInstance(keyStore.getType());
                    char[] storePassword = passwordOrNull(keyStore.storePasswordProvider());
                    try (FileInputStream fileInputStream = new FileInputStream(keyStore.storeFile())) {
                        store.load(fileInputStream, storePassword);
                    }
                    KeyManagerFactory instance = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
                    char[] keyPassword = passwordOrNull(keyStore.keyPasswordProvider());
                    keyPassword = keyPassword == null ? storePassword : keyPassword;
                    instance.init(store, keyPassword);
                    return instance.getKeyManagers();
                }
                catch (Exception e) {
                    throw new SslConfigurationException(e);
                }
            }

            @Override
            public KeyManager[] visit(KeyPairSet keyPairSet) {
                throw new SslConfigurationException("KeyPairSet is not supported by vault KMS yet");
            }

            @Nullable
            private static char[] passwordOrNull(PasswordProvider value) {
                return Optional.ofNullable(value).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
            }
        });
    }

    @NonNull
    private static SSLContext getSslContext(TrustManager[] trustManagers, KeyManager[] keyManagers) {
        try {
            if (trustManagers == null && keyManagers == null) {
                return SSLContext.getDefault();
            }
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(keyManagers, trustManagers, new SecureRandom());
            return context;
        }
        catch (Exception e) {
            throw new SslConfigurationException(e);
        }
    }

    @VisibleForTesting
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

            @Override
            public TrustManager[] visit(PlatformTrustProvider platformTrustProviderTls) {
                return getDefaultTrustManagers();
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
