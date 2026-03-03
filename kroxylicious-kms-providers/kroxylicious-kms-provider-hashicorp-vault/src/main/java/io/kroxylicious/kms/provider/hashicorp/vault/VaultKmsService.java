/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.time.Duration;
import java.util.Objects;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of HashiCorp Vault.
 */
@Plugin(configType = Config.class)
public class VaultKmsService implements KmsService<Config, String, VaultEdek> {

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile @Nullable Config config;

    @Override
    public void initialize(Config config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public VaultKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());
        return new VaultKms(config.vaultTransitEngineUrl(), config.vaultToken().getProvidedPassword(), Duration.ofSeconds(20),
                tlsConfigurator);
    }

}
