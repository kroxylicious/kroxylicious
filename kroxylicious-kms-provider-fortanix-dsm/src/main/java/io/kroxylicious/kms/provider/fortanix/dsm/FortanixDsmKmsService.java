/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.fortanix.dsm;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

import io.kroxylicious.kms.provider.fortanix.dsm.config.Config;
import io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProvider;
import io.kroxylicious.kms.provider.fortanix.dsm.session.SessionProviderFactory;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} backed by <a href="https://www.fortanix.com/platform/data-security-manager">Fortanix DSM</a>.
 */
@Plugin(configType = Config.class)
public class FortanixDsmKmsService implements KmsService<Config, String, FortanixDsmKmsEdek> {

    private final SessionProviderFactory sessionProviderFactory;
    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile Config config;
    private SessionProvider sessionProvider;

    public FortanixDsmKmsService() {
        this(SessionProviderFactory.DEFAULT);
    }

    @VisibleForTesting
    FortanixDsmKmsService(@NonNull SessionProviderFactory sessionProviderFactory) {
        this.sessionProviderFactory = Objects.requireNonNull(sessionProviderFactory);
    }

    @Override
    public void initialize(@NonNull Config config) {
        Objects.requireNonNull(config);
        this.config = config;
        this.sessionProvider = sessionProviderFactory.createSessionProvider(config);
    }

    @NonNull
    @Override
    public FortanixDsmKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        return new FortanixDsmKms(config.endpointUrl(),
                sessionProvider, Duration.ofSeconds(20), config.sslContext());
    }

    @Override
    public void close() {
        Optional.ofNullable(sessionProvider).ifPresent(SessionProvider::close);
    }
}
