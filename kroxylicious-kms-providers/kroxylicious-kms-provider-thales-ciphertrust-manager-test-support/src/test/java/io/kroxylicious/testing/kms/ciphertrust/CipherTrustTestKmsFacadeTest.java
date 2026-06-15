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

import static org.assertj.core.api.Assertions.assertThat;

class CipherTrustTestKmsFacadeTest {

    private CipherTrustTestKmsFacade facade;

    @BeforeEach
    void setUp() {
        facade = new CipherTrustTestKmsFacade();
        facade.start();
    }

    @AfterEach
    void tearDown() {
        if (facade != null) {
            facade.close();
        }
    }

    @Test
    void shouldProvideKmsServiceClass() {
        // When/Then
        assertThat(facade.getKmsServiceClass()).isEqualTo(CipherTrustKmsService.class);
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
        // When
        var kekManager = facade.getTestKekManager();

        // Then
        assertThat(kekManager).isNotNull();
    }
}
