/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm.session;

import java.net.http.HttpClient;
import java.util.Objects;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.service.KmsException;

/**
 * Factory for the Fortanix SessionProviders
 */
@FunctionalInterface
public interface SessionProviderFactory {

    /**
     * Creates a session provider.
     *
     * @param config configuration
     * @param client http client
     * @return session provider.
     */
    SessionProvider createSessionProvider(Config config,
                                          HttpClient client);

    /**
     * Default session provider implementation.
     */
    SessionProviderFactory DEFAULT = new SessionProviderFactory() {
        @Override
        public SessionProvider createSessionProvider(Config config,
                                                     HttpClient client) {
            Objects.requireNonNull(config);
            Objects.requireNonNull(client);
            var configException = new KmsException("Config %s must define exactly one session provider".formatted(config));
            if (config.apiKeySessionProviderConfig() != null) {
                return new ApiKeySessionProvider(config, client);
            }
            else {
                throw configException;
            }
        }
    };
}
