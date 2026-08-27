/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.hashicorp.vault;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import io.kroxylicious.kms.provider.hashicorp.vault.config.Config;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Integration tests verifying that a real HashiCorp Vault instance returns {@code latest_version}
 * and that rotating a key causes {@link VaultKms#resolveAlias} to return a different
 * {@link WrappingKey} with a higher version number.
 */
class VaultKmsRotationIT {

    private static final String VAULT_TOKEN = "token";
    private TestVault vault;
    private VaultKms kms;

    @BeforeEach
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();
        vault = TestVault.start();
        var config = new Config(vault.getEndpoint(), new InlinePassword(VAULT_TOKEN), null);
        var service = new VaultKmsService();
        service.initialize(config);
        kms = service.buildKms();
    }

    @AfterEach
    void afterEach() {
        if (vault != null) {
            vault.close();
        }
    }

    @Test
    void resolveAliasReturnsLatestVersionFromRealVault() {
        // Given
        var keyName = "rotation-test-key";
        vault.createKek(keyName);

        // When
        var wrappingKey = kms.resolveAlias(keyName).toCompletableFuture().join();

        // Then - real Vault must populate latest_version (proves the field is available)
        assertThat(wrappingKey.name()).isEqualTo(keyName);
        assertThat(wrappingKey.version()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void rotationIncreasesVersionAndChangesWrappingKeyIdentity() {
        // Given
        var keyName = "rotation-identity-key";
        vault.createKek(keyName);

        // When
        var before = kms.resolveAlias(keyName).toCompletableFuture().join();
        vault.rotateKek(keyName);
        var after = kms.resolveAlias(keyName).toCompletableFuture().join();

        // Then
        assertThat(before.name()).isEqualTo(after.name());
        assertThat(after.version()).isGreaterThan(before.version());
        assertThat(before)
                .as("WrappingKey identity must differ after rotation so EncryptionDekCache sees a cache miss")
                .isNotEqualTo(after);
    }

}
