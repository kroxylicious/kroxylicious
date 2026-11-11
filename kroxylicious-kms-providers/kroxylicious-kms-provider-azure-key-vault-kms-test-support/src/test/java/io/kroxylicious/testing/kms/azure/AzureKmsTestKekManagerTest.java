/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.azure;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.kroxylicious.testing.kms.azure.AbstractAzureKeyVaultKmsTestKmsFacade.AzureKmsTestKekManager;

import static org.assertj.core.api.Assertions.assertThat;

class AzureKmsTestKekManagerTest {

    @Test
    void cloudConstructorBuildsKeyClientForVaultUrlWithoutContactingAzure() {
        // Given
        String tenantId = UUID.randomUUID().toString();
        String clientId = UUID.randomUUID().toString();
        String clientSecret = UUID.randomUUID().toString();
        String vaultBaseUrl = "https://my-key-vault.vault.azure.net";

        // When
        var manager = new AzureKmsTestKekManager(tenantId, clientId, clientSecret, vaultBaseUrl);

        // Then
        assertThat(manager.keyClient).isNotNull();
        assertThat(manager.keyClient.getVaultUrl()).isEqualTo(vaultBaseUrl);
    }
}
