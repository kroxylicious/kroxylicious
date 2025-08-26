/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.provider.azure.config.auth.EntraIdentityConfig;
import io.kroxylicious.kms.service.Kms;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AzureKeyVaultKmsServiceTest {

    @Test
    void wholeLifeCycle() {
        try (AzureKeyVaultKmsService service = new AzureKeyVaultKmsService()) {
            EntraIdentityConfig entraIdentity = new EntraIdentityConfig(null, "tenant", new InlinePassword("abc"), new InlinePassword("def"), null, null);
            service.initialize(new AzureKeyVaultConfig(entraIdentity, "default", "vault.azure.net", null, null, null));
            Kms<WrappingKey, AzureKeyVaultEdek> kms = service.buildKms();
            assertThat(kms).isNotNull();
        }
    }

    @Test
    void buildWithoutInitialize() {
        try (AzureKeyVaultKmsService service = new AzureKeyVaultKmsService()) {
            assertThatThrownBy(service::buildKms).isInstanceOf(IllegalStateException.class).hasMessage("client has not been initialized");
        }
    }

    @Test
    void getConfigWithoutInitialize() {
        try (AzureKeyVaultKmsService service = new AzureKeyVaultKmsService()) {
            assertThatThrownBy(service::getConfig).isInstanceOf(IllegalStateException.class).hasMessage("config has not been initialized");
        }
    }

}