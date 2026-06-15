/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustKmsService;
import io.kroxylicious.kms.provider.thales.ciphertrust.config.Config;
import io.kroxylicious.kms.service.UnknownAliasException;
import io.kroxylicious.testing.kms.TestKekManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CipherTrustTestKmsFacadeTest {

    private CipherTrustTestKmsFacade facade;
    private TestKekManager manager;

    @BeforeEach
    void setUp() {
        facade = new CipherTrustTestKmsFacade();
        facade.start();
        manager = facade.getTestKekManager();
    }

    @AfterEach
    void tearDown() {
        if (facade != null) {
            facade.close();
        }
    }

    @Test
    void classAndConfig() {
        // When/Then
        assertThat(facade.getKmsServiceClass()).isEqualTo(CipherTrustKmsService.class);
        assertThat(facade.getKmsServiceConfig()).isInstanceOf(Config.class);
    }

    @Test
    void shouldGenerateValidConfig() {
        // When
        var config = facade.getKmsServiceConfig();

        // Then
        assertThat(config).isNotNull();
        assertThat(config.endpointUrl()).isNotNull();
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().username()).isNotEmpty();
    }

    @Test
    void shouldProvideTestKekManager() {
        // When/Then
        assertThat(manager).isNotNull();
    }

    @Test
    void generateKek() {
        // Given
        String alias = "test-key";

        // When
        manager.generateKek(alias);

        // Then
        assertThat(manager.read(alias)).isNotNull();
    }

    @Test
    void rotateKek() {
        // Given
        String alias = "rotate-test-key";
        manager.generateKek(alias);

        // When/Then
        assertThatNoException().isThrownBy(() -> manager.rotateKek(alias));
    }

    @Test
    void deleteKek() {
        // Given
        String alias = "delete-test-key";
        manager.generateKek(alias);

        // When
        manager.deleteKek(alias);

        // Then
        assertThatThrownBy(() -> manager.read(alias))
                .isInstanceOf(UnknownAliasException.class);
    }
}
