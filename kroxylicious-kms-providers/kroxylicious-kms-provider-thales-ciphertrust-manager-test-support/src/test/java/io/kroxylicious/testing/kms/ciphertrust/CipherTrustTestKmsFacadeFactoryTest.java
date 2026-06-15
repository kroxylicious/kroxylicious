/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.kms.ciphertrust;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CipherTrustTestKmsFacadeFactoryTest {

    @Test
    void shouldBuildFacade() {
        // Given
        var factory = new CipherTrustTestKmsFacadeFactory();

        // When
        var facade = factory.build();

        // Then
        assertThat(facade)
                .isInstanceOf(CipherTrustTestKmsFacade.class);
    }
}
