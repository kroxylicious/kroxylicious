/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.kms.provider.hashicorp.vault.config.JdkTls;
import io.kroxylicious.kms.provider.hashicorp.vault.config.SslConfigurationException;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of HashiCorp Vault.
 */
@Plugin(configType = Config.class)
public class VaultKmsService implements KmsService<Config, String, VaultEdek> {

    @SuppressWarnings("java:S3077") // KMS services are thread safe. As Config is immutable, volatile is sufficient to ensure its safe publication between threads.
    private volatile Config config;

    @NonNull
    public static SSLContext sslContext(Config config) {
        try {
            if (config.tls() == null) {
                return SSLContext.getDefault();
            }
            else {
                return new JdkTls(config.tls()).sslContext();
            }
        }
        catch (NoSuchAlgorithmException e) {
            throw new SslConfigurationException(e);
        }
    }

    @Override
    public void initialize(@NonNull Config config) {
        Objects.requireNonNull(config);
        this.config = config;
    }

    @NonNull
    @Override
    public VaultKms buildKms() {
        Objects.requireNonNull(config, "KMS service not initialized");
        return new VaultKms(config.vaultTransitEngineUrl(), config.vaultToken().getProvidedPassword(), Duration.ofSeconds(20),
                sslContext(config));
    }

}
