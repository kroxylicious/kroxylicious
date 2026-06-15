/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import org.junit.jupiter.api.Test;

import io.kroxylicious.kms.provider.thales.ciphertrust.CipherTrustKmsService;

import static org.assertj.core.api.Assertions.assertThat;

class CipherTrustTestKmsFacadeTest {

    @Test
    void shouldStartAndStop() {
        // Given
        var facade = new CipherTrustTestKmsFacade();

        // When
        facade.start();

        // Then
        assertThat(facade.getKmsServiceClass()).isEqualTo(CipherTrustKmsService.class);
        assertThat(facade.getKmsServiceConfig()).isNotNull();
        assertThat(facade.getTestKekManager()).isNotNull();

        // Cleanup
        facade.close();
    }

    @Test
    void shouldGenerateValidConfig() {
        // Given
        var facade = new CipherTrustTestKmsFacade();
        facade.start();

        // When
        var config = facade.getKmsServiceConfig();

        // Then
        assertThat(config.endpointUrl()).isNotNull();
        assertThat(config.userCredentials()).isNotNull();
        assertThat(config.userCredentials().username()).isNotEmpty();

        facade.close();
    }

    @Test
    void shouldProvideTestKekManager() {
        // Given
        var facade = new CipherTrustTestKmsFacade();
        facade.start();

        // When
        var kekManager = facade.getTestKekManager();

        // Then
        assertThat(kekManager).isNotNull();

        facade.close();
    }
}
