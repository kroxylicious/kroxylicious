/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import java.net.URI;
import java.util.Objects;

import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * {@inheritDoc}
 * <br/>
 * An implementation of the {@link KmsService} interface backed by a remote instance of Hashicorp Vault.
 */
@Plugin(configType = VaultKmsService.Config.class)
public class VaultKmsService implements KmsService<VaultKmsService.Config, String, VaultEdek> {
    public record Config(URI vaultUrl,
                         String vaultToken) {
        public Config {
            Objects.requireNonNull(vaultUrl);
            Objects.requireNonNull(vaultToken);
        }
    }

    @NonNull
    @Override
    public VaultKms buildKms(Config options) {
        return new VaultKms(options.vaultUrl(), options.vaultToken());
    }

}
