/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

import javax.net.ssl.SSLContext;

import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.config.tls.Tls;
import io.kroxylicious.proxy.filter.FilterFactoryContext;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * An implementation of the {@link KmsService} interface backed by a remote instance of HashiCorp Vault.
 */
@Plugin(configType = VaultKmsService.Config.class)
public class VaultKmsService implements KmsService<VaultKmsService.Config, String, VaultEdek> {
    /**
     * Configuration for the Vault KMS service.
     * @param vaultUrl vault url
     * @param vaultToken vault token.
     */
    public record Config(URI vaultUrl,
                         String vaultToken,
                         Tls tls) {
        public Config {
            Objects.requireNonNull(vaultUrl);
            Objects.requireNonNull(vaultToken);
        }
    }

    @NonNull
    @Override
    public VaultKms buildKms(Config options, FilterFactoryContext context) {
        SSLContext sslContext = null;
        if (options.tls != null) {
            sslContext = context.clientSslContext(options.tls);
        }
        return new VaultKms(options.vaultUrl(), options.vaultToken(), Duration.ofSeconds(20), sslContext);
    }

}
