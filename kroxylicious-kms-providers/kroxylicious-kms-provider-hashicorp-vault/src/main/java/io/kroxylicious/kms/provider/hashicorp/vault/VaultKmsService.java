/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.testing.kms.tls.TlsHttpClientConfigurator;

import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of HashiCorp Vault.
 */
@Plugin(configType = Config.class)
public class VaultKmsService implements KmsService<Config, WrappingKey, VaultEdek> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VaultKmsService.class);

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile @Nullable Config config;

    /**
     * Creates the HashiCorp Vault KMS service.
     */
    public VaultKmsService() {
        // Intentionally empty
    }

    @Override
    public void initialize(Config config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @Override
    public VaultKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        var tlsConfigurator = new TlsHttpClientConfigurator(config.tls());

        var credentials = config.credentials();
        String vaultNamespace = config.vaultNamespace();
        String transitEnginePath = config.transitEnginePath();
        URI vaultUrl = config.vaultUrl();

        URI transitEngineUri = buildVaultEndpointUri(vaultUrl, transitEnginePath);
        LOGGER.atInfo().addKeyValue("transitEngineUri", transitEngineUri).log("Resolved Vault Transit Engine URL");

        VaultTokenProvider tokenProvider;
        if (credentials.kubernetes() != null) {
            throw new UnsupportedOperationException("Kubernetes authentication is not supported yet");
        }
        else {
            tokenProvider = new StaticTokenProvider(credentials.vaultToken().token().getProvidedPassword());
        }

        return new VaultKms(transitEngineUri, vaultNamespace, tokenProvider, Duration.ofSeconds(20),
                tlsConfigurator);
    }

    private static URI buildVaultEndpointUri(URI vaultUrl, String path) {
        String base = vaultUrl.toString();
        if (!base.endsWith("/")) {
            base += "/";
        }
        base += "v1/";
        base += path.endsWith("/") ? path : path + "/";
        return URI.create(base);
    }

}
