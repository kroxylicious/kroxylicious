/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.kms.provider.thales.ciphertrust.auth.BearerTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.auth.CachingBearerTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.auth.ClientCertificateTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.auth.UserAuthenticationTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * KMS service implementation for Thales CipherTrust Manager.
 * <p>
 * Manages the lifecycle of {@link CipherTrustKms} instances, including
 * configuration validation, authentication token service creation, and
 * KMS instance building.
 * </p>
 */
@Plugin(configType = Config.class)
public class CipherTrustKmsService implements KmsService<Config, WrappingKey, CipherTrustEdek> {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    @Nullable
    private volatile Config config;

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    @Nullable
    private volatile BearerTokenService tokenService;

    /**
     * Creates a new CipherTrust KMS service.
     */
    public CipherTrustKmsService() {
        // deliberately empty
    }

    @Override
    public void initialize(Config config) {
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
        this.tokenService = createTokenService(config);
    }

    @Override
    public Kms<WrappingKey, CipherTrustEdek> buildKms() {
        var c = Objects.requireNonNull(config, "KMS service not initialized");
        var ts = Objects.requireNonNull(tokenService, "Bearer token server not initialized");

        var tlsConfigurator = new TlsHttpClientConfigurator(c.tls());

        return new CipherTrustKms(
                c.endpointUrl(),
                ts,
                DEFAULT_TIMEOUT,
                tlsConfigurator);
    }

    private BearerTokenService createTokenService(Config config) {
        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());
        BearerTokenService delegate;

        if (config.userCredentials() != null) {
            // User authentication with username/password
            var userCreds = config.userCredentials();
            delegate = new UserAuthenticationTokenService(
                    config.endpointUrl(),
                    userCreds.username(),
                    userCreds.password().getProvidedPassword(),
                    userCreds.domain(),
                    DEFAULT_TIMEOUT,
                    tlsConfigurator);
        }
        else if (config.clientCredentials() != null) {
            // Client certificate authentication
            var clientCreds = config.clientCredentials();
            delegate = new ClientCertificateTokenService(
                    config.endpointUrl(),
                    clientCreds.clientId(),
                    DEFAULT_TIMEOUT,
                    tlsConfigurator);
        }
        else {
            // Cannot happen, the Config ctor makes the enforcement.
            throw new IllegalStateException("Must supply either userCredentials or clientCredentials");
        }

        // Wrap with caching token service for automatic token refresh/re-authentication
        return new CachingBearerTokenService(delegate, Clock.systemUTC());
    }

    @Override
    public void close() {
        Optional.ofNullable(tokenService).ifPresent(BearerTokenService::close);
    }
}
