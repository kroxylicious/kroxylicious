/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.thales.ciphertrust;

import java.time.Clock;
import java.time.Duration;
import java.util.Objects;

import io.kroxylicious.kms.provider.thales.ciphertrust.auth.BearerTokenService;
import io.kroxylicious.kms.provider.thales.ciphertrust.auth.CachingBearerTokenService;
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
public class CipherTrustKmsService implements KmsService<Config, String, CipherTrustEdek> {

    private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(20);

    /**
     * Creates a new CipherTrust KMS service.
     */
    public CipherTrustKmsService() {
    }

    @Nullable
    private volatile Config config;

    @Nullable
    private volatile BearerTokenService tokenService;

    @Override
    public void initialize(Config config) {
        Objects.requireNonNull(config, "config cannot be null");
        this.config = config;
    }

    @Override
    public Kms<String, CipherTrustEdek> buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");

        // Create token service if not already created
        if (tokenService == null) {
            synchronized (this) {
                if (tokenService == null) {
                    tokenService = createTokenService(config);
                }
            }
        }

        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());

        return new CipherTrustKms(
                config.endpointUrl(),
                tokenService,
                DEFAULT_TIMEOUT,
                tlsConfigurator);
    }

    private BearerTokenService createTokenService(Config config) {
        BearerTokenService delegate;

        if (config.userCredentials() != null) {
            // User authentication
            var userCreds = config.userCredentials();
            var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());

            delegate = new UserAuthenticationTokenService(
                    config.endpointUrl(),
                    userCreds.username(),
                    userCreds.password().getProvidedPassword(),
                    DEFAULT_TIMEOUT,
                    tlsConfigurator);
        }
        else if (config.clientCredentials() != null) {
            // Client authentication - not yet implemented
            throw new UnsupportedOperationException("Client authentication is not yet implemented");
        }
        else {
            // Should not reach here due to Config validation, but belt-and-suspenders
            throw new IllegalStateException("No credentials configured");
        }

        // Wrap with caching token service for automatic token refresh
        return new CachingBearerTokenService(delegate, Clock.systemUTC());
    }

    @Override
    public void close() {
        if (tokenService != null) {
            tokenService.close();
        }
    }
}
