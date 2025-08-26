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

    @CsvSource({ "RSA,RSA-OAEP-256", "RSA_HSM,RSA-OAEP-256", "OCT,A256GCM", "OCT_HSM,A256GCM" })
    @ParameterizedTest
    void wrapAlgorithm(SupportedKeyType supportedKeyType, String wrapAlgorithm) {
        assertThat(supportedKeyType.getWrapAlgorithm()).isEqualTo(wrapAlgorithm);
    }

    @CsvSource({ "RSA,RSA", "RSA-HSM,RSA_HSM", "oct,OCT", "oct-HSM,OCT_HSM" })
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