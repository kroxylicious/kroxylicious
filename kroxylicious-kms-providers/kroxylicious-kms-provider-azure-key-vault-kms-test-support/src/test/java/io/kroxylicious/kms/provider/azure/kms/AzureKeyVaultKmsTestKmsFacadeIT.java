/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.kms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;

import io.kroxylicious.kms.provider.azure.AzureKeyVaultEdek;
import io.kroxylicious.kms.provider.azure.AzureKeyVaultKmsService;
import io.kroxylicious.kms.provider.azure.WrappingKey;
import io.kroxylicious.kms.provider.azure.config.AzureKeyVaultConfig;
import io.kroxylicious.kms.service.AbstractTestKmsFacadeTest;
import io.kroxylicious.kms.service.UnknownAliasException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

class AzureKeyVaultKmsTestKmsFacadeIT extends AbstractTestKmsFacadeTest<AzureKeyVaultConfig, WrappingKey, AzureKeyVaultEdek> {

    AzureKeyVaultKmsTestKmsFacadeIT() {
        super(new AzureKeyVaultKmsTestKmsFacadeFactory());
    }

    @BeforeEach
    void beforeEach() {
        assumeThat(DockerClientFactory.instance().isDockerAvailable()).withFailMessage("docker unavailable").isTrue();
    }

    @Test
    void classAndConfig() {
        try (var facade = factory.build()) {
            facade.start();
            assertThat(facade.getKmsServiceClass()).isEqualTo(AzureKeyVaultKmsService.class);
            assertThat(facade.getKmsServiceConfig()).isInstanceOf(AzureKeyVaultConfig.class);
        }
    }

    @Test
    void generateKekFailsIfAliasExists() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();
            manager.generateKek(ALIAS);

            assertThatThrownBy(() -> manager.generateKek(ALIAS))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("key '" + ALIAS + "' already exists");
        }
    }

    @Test
    void rotateKekFailsIfAliasDoesNotExist() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();

            assertThatThrownBy(() -> manager.rotateKek(ALIAS))
                    .isInstanceOf(UnknownAliasException.class);
        }
    }

    @Test
    void deleteKekFailsIfAliasDoesNotExist() {
        try (var facade = factory.build()) {
            facade.start();
            var manager = facade.getTestKekManager();

            assertThatThrownBy(() -> manager.deleteKek(ALIAS))
                    .isInstanceOf(UnknownAliasException.class);
        }
    }
}
