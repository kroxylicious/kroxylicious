/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Builder;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.proxy.config.secret.PasswordProvider;
import io.kroxylicious.proxy.config.tls.AllowDeny;
import io.kroxylicious.proxy.config.tls.InsecureTls;
import io.kroxylicious.proxy.config.tls.KeyPair;
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
 * Responsible for applying TLS configuration to a {@link HttpClient.Builder}.
 */
public class TlsHttpClientConfigurator implements UnaryOperator<Builder> {

    private static final Logger LOGGER = LoggerFactory.getLogger(TlsHttpClientConfigurator.class);

    private static final SSLContext PLATFORM_SSL_CONTEXT;

    static {
        try {
            PLATFORM_SSL_CONTEXT = SSLContext.getDefault();
        }
        catch (NoSuchAlgorithmException nsae) {
            var e = new ExceptionInInitializerError("Failed to access default SSL context for platform");
            e.initCause(nsae);
            throw e;
        }
    }

    private static final X509TrustManager INSECURE_TRUST_MANAGER = new InsecureTrustManager();

    private static final TrustManager[] INSECURE_TRUST_MANAGERS = { INSECURE_TRUST_MANAGER };
    @Nullable
    private final Tls tls;

    /**
     * Creates a TLS configurator.
     *
     * @param tls tls parameters
     */
    public TlsHttpClientConfigurator(@Nullable Tls tls) {
        if (tls != null && tls.key() != null) {
            LOGGER.warn("TLS key material is currently not supported by this client");
        }
        this.tls = tls;
    }

    private SSLContext sslContext() {
        try {
            if (tls == null || (tls.trust() == null && tls.key() == null)) {
                return PLATFORM_SSL_CONTEXT;
            }
            else {
                TrustManager[] trustManagers = null;
                KeyManager[] keyManagers = null;

                if (tls.trust() != null) {
                    trustManagers = getTrustManagers(tls.trust());
                }
                if (tls.key() != null) {
                    keyManagers = getKeyManagers(tls.key());
                }

                SSLContext context = SSLContext.getInstance("TLS");
                context.init(keyManagers, trustManagers, new SecureRandom());
                return context;
            }
        }
        catch (Exception e) {
            throw new SslConfigurationException(e);
        }
    }

    private SSLParameters sslParameters() {
        var defaultSslParameters = PLATFORM_SSL_CONTEXT.getDefaultSSLParameters();
        if (tls == null || (tls.protocols() == null && tls.cipherSuites() == null)) {
            return defaultSslParameters;
        }

        var supportedSSLParameters = PLATFORM_SSL_CONTEXT.getSupportedSSLParameters();

        var protocols = applyRestriction("protocol", tls.protocols(), defaultSslParameters, supportedSSLParameters, SSLParameters::getProtocols);
        var cipherSuites = applyRestriction("cipher suite", tls.cipherSuites(), defaultSslParameters, supportedSSLParameters, SSLParameters::getCipherSuites);

        defaultSslParameters.setProtocols(protocols);
        defaultSslParameters.setCipherSuites(cipherSuites);

        return defaultSslParameters;
    }

    @NonNull
    private String[] applyRestriction(String subject, AllowDeny<String> allowDeny, SSLParameters defaultSslParameters, SSLParameters supportedSSLParameters,
                                      Function<SSLParameters, String[]> sslParametersAccessor) {
        var result = Arrays.stream(sslParametersAccessor.apply(defaultSslParameters)).toList();
        if (allowDeny != null) {
            var supported = Arrays.stream(sslParametersAccessor.apply(supportedSSLParameters)).collect(Collectors.toSet());
            var allowed = allowDeny.allowed();

            if (allowed != null && !allowed.isEmpty()) {
                allowed.stream()
                        .filter(Predicate.not(supported::contains))
                        .forEach(unsupported -> LOGGER.warn("Ignoring allowed {} '{}' as it is not recognized by this platform (supported: {})",
                                subject, unsupported, supported));
                result = allowed.stream()
                        .filter(supported::contains)
                        .toList();
            }

            var denied = allowDeny.denied();
            if (denied != null) {
                denied.stream()
                        .filter(Predicate.not(supported::contains))
                        .forEach(unsupported -> LOGGER.warn(
                                "Ignoring denied {}} '{}' as it is not recognized by this platform (supported: {})",
                                subject, unsupported, supported));
                result = result.stream()
                        .filter(Predicate.not(denied::contains))
                        .toList();
            }

            if (result.isEmpty()) {
                throw new SslConfigurationException(
                        "The configuration you have in place has resulted in no %ss being available. Allowed: %s, Denied: %s".formatted(subject, allowed, denied));
            }
        }
        return result.toArray(new String[]{});
    }

    @VisibleForTesting
    static KeyManager[] getKeyManagers(KeyProvider key) {
        return key.accept(new KeyProviderVisitor<>() {
            @Override
            public KeyManager[] visit(KeyPair keyPair) {
                throw new SslConfigurationException("KeyPair is not supported by this client");
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

            @Nullable
            private static char[] passwordOrNull(PasswordProvider value) {
                return Optional.ofNullable(value).map(PasswordProvider::getProvidedPassword).map(String::toCharArray).orElse(null);
            }
        });
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

    /**
     * Applies TLS configuration to the supplied {@link Builder}.  If there is no
     * TLS configuration to apply, this operation is a no-op.
     *
     * @param builder HTTP client builder
     * @return HTTP client builder
     */
    @Override
    public Builder apply(@NonNull Builder builder) {
        Objects.requireNonNull(builder);
        builder.sslContext(sslContext())
                .sslParameters(sslParameters());
        return builder;
    }
}
