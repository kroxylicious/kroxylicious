/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import java.security.SecureRandom;
import java.time.Clock;

import io.kroxylicious.kms.provider.azure.auth.BearerTokenService;
import io.kroxylicious.kms.provider.azure.auth.CachingBearerTokenService;
import io.kroxylicious.kms.provider.azure.auth.ManagedIdentityAccessTokenService;
import io.kroxylicious.kms.provider.azure.auth.OauthClientCredentialsTokenService;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.keyvault.KeyVaultClient;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.kms.service.KmsService;
import io.kroxylicious.proxy.plugin.Plugin;
import io.kroxylicious.proxy.tag.VisibleForTesting;

import edu.umd.cs.findbugs.annotations.Nullable;

@Plugin(configType = AzureKeyVaultConfig.class)
public class AzureKeyVaultKmsService implements KmsService<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    @Nullable
    private KeyVaultClient client;
    @Nullable
    private AzureKeyVaultConfig config;

    @Override
    public void initialize(AzureKeyVaultConfig config) {
        this.config = config;
        Clock clock = Clock.systemUTC();
        BearerTokenService delegateService;
        if (config.entraIdentity() != null) {
            delegateService = new OauthClientCredentialsTokenService(config.entraIdentity(), clock);
        }
        else if (config.managedIdentity() != null) {
            delegateService = new ManagedIdentityAccessTokenService(config.managedIdentity(), clock);
        }
        else {
            throw new IllegalStateException("No identity provider configured");
        }
        BearerTokenService service = new CachingBearerTokenService(delegateService, clock);
        client = new KeyVaultClient(service, config);
    }

    @Override
    public Kms<WrappingKey, AzureKeyVaultEdek> buildKms() throws IllegalStateException {
        if (client == null) {
            throw new IllegalStateException("client has not been initialized");
        }
        return new AzureKeyVaultKms(client, config.keyVaultName(), SECURE_RANDOM);
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
        }
    }

    @VisibleForTesting
    AzureKeyVaultConfig getConfig() {
        if (config == null) {
            throw new IllegalStateException("config has not been initialized");
        }
        return config;
    }
}
