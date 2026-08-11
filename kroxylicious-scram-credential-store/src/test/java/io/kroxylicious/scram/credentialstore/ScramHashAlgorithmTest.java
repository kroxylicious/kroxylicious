/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScramHashAlgorithmTest {

    @Test
    void fromAlgorithmNameShouldReturnSha256() {
        // When
        ScramHashAlgorithm result = ScramHashAlgorithm.fromAlgorithmName("SHA-256");

        // Then
        assertThat(result).isEqualTo(ScramHashAlgorithm.SHA_256);
    }

    @Test
    void fromAlgorithmNameShouldReturnSha512() {
        // When
        ScramHashAlgorithm result = ScramHashAlgorithm.fromAlgorithmName("SHA-512");

        // Then
        assertThat(result).isEqualTo(ScramHashAlgorithm.SHA_512);
    }

    @Test
    void fromAlgorithmNameShouldRejectUnsupportedAlgorithm() {
        // When / Then
        assertThatThrownBy(() -> ScramHashAlgorithm.fromAlgorithmName("SHA-384"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("SHA-384");
    }

}
