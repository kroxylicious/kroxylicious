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

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * Factory for the Fortanix SessionProviders
 */
@FunctionalInterface
public interface SessionProviderFactory {

    /**
     * Creates a session provider.
     *
     * @param config configuration
     * @param client
     * @return session provider.
     */
    @NonNull
    SessionProvider createSessionProvider(@NonNull Config config, HttpClient client);

    SessionProviderFactory DEFAULT = new SessionProviderFactory() {
        @NonNull
        @Override
        public SessionProvider createSessionProvider(@NonNull Config config, @NonNull HttpClient client) {
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
