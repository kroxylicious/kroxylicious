/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure.keyvault;

import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class SupportedKeyTypeTest {

    @CsvSource({ "RSA,false", "AES,true" })
    @ParameterizedTest
    void quantumResistant(SupportedKeyType supportedKeyType, boolean resistant) {
        assertThat(supportedKeyType.isQuantumResistant()).isEqualTo(resistant);
    }

    @CsvSource({ "RSA,RSA-OAEP-256", "AES,A256GCM" })
    @ParameterizedTest
    void wrapAlgorithm(SupportedKeyType supportedKeyType, String wrapAlgorithm) {
        assertThat(supportedKeyType.getWrapAlgorithm()).isEqualTo(wrapAlgorithm);
    }

    @CsvSource({ "RSA,RSA", "RSA-HSM,RSA", "oct,AES", "oct-HSM,AES" })
    @ParameterizedTest
    void fromKeyType(String keyType, SupportedKeyType supportedKeyType) {
        Optional<SupportedKeyType> type = SupportedKeyType.fromKeyType(keyType);
        assertThat(type).contains(supportedKeyType);
    }

    @CsvSource({ "EC", "EC-HSM", "unknown-new-type" })
    @ParameterizedTest
    void fromUnsupportedKeyType(String keyType) {
        Optional<SupportedKeyType> type = SupportedKeyType.fromKeyType(keyType);
        assertThat(type).isEmpty();
    }
}