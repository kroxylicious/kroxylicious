/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.time.Duration;
import java.util.Objects;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.config.VaultKmsConfigV1;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of HashiCorp Vault.
 */
@Plugin(configVersion = "", configType = Config.class)
@Plugin(configVersion = "v1", configType = VaultKmsConfigV1.class)
public class VaultKmsService implements KmsService<Object, String, VaultEdek> {

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile @Nullable Config legacyConfig;

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile @Nullable VaultKmsConfigV1 v1Config;

    @Override
    public void initialize(Object config) {
        Objects.requireNonNull(config);
        if (config instanceof VaultKmsConfigV1 v1) {
            this.v1Config = v1;
        }
        else if (config instanceof Config legacy) {
            this.legacyConfig = legacy;
        }
        else {
            throw new IllegalArgumentException("Unsupported config type: " + config.getClass().getName());
        }
    }

    @Override
    public VaultKms buildKms() {
        if (v1Config != null) {
            return buildKmsV1(v1Config);
        }
        else if (legacyConfig != null) {
            return buildKmsLegacy(legacyConfig);
        }
        throw new IllegalStateException("KMS service not initialized");
    }

    private VaultKms buildKmsLegacy(Config config) {
        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());
        return new VaultKms(config.vaultTransitEngineUrl(), config.vaultToken().getProvidedPassword(), Duration.ofSeconds(20),
                tlsConfigurator);
    }

    private VaultKms buildKmsV1(VaultKmsConfigV1 config) {
        // TODO: resolve KeyMaterialProvider/TrustMaterialProvider from the ResolvedPluginRegistry
        // to build SSLContext from the referenced key/trust material plugin instances.
        // This requires a mechanism for KmsService to access the registry (not yet available).
        return new VaultKms(config.vaultTransitEngineUrl(), config.vaultToken(), Duration.ofSeconds(20),
                builder -> builder);
    }

}
