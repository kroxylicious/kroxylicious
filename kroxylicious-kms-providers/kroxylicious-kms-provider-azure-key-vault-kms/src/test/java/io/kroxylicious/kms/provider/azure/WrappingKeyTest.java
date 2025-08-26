/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.kms.provider.azure;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType;

import edu.umd.cs.findbugs.annotations.Nullable;

import static io.kroxylicious.kms.provider.azure.keyvault.SupportedKeyType.OCT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WrappingKeyTest {

    static final String KEY_NAME = "name";
    static final String KEY_VERSION = "version";

    @Test
    void keyVersion() {
        WrappingKey wrappingKey = new WrappingKey(KEY_NAME, KEY_VERSION, OCT);
        assertThat(wrappingKey.keyName()).isEqualTo(KEY_NAME);
        assertThat(wrappingKey.keyVersion()).isEqualTo(KEY_VERSION);
        assertThat(wrappingKey.supportedKeyType()).isEqualTo(OCT);
    }

    @CsvSource(nullValues = "null", value = { "null, version, OCT, keyName is null",
            "name, null, OCT, keyVersion is null, name, version, null, supportedKeyType is null" })
    @ParameterizedTest
    void invalidConstructorArgs(@Nullable String name, @Nullable String version, @Nullable String supportedKeyType, String message) {
        SupportedKeyType supportedKeyType1 = supportedKeyType == null ? null : SupportedKeyType.valueOf(supportedKeyType);
        assertThatThrownBy(() -> new WrappingKey(name, version, supportedKeyType1)).isInstanceOf(
                NullPointerException.class).hasMessage(message);
    }

    @Test
    void parse() {
        WrappingKey name = WrappingKey.parse("CreateSoftKeyTest", "https://myvault.vault.azure.net/keys/CreateSoftKeyTest/78deebed173b48e48f55abf87ed4cf71", OCT);
        assertThat(name.keyName()).isEqualTo("CreateSoftKeyTest");
        assertThat(name.keyVersion()).isEqualTo("78deebed173b48e48f55abf87ed4cf71");
    }

    @Test
    void parseNonUriId() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "z banana/zc", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("failed to parse keyId 'z banana/zc' as a URI");
    }

    @Test
    void parseUriWithNoSlashInPath() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://www.banana.com", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '' must contain a '/'");
    }

    @Test
    void parseUriWithOnlySlashInPath() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://www.banana.com/", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '/' must not end with '/'");
    }

    @Test
    void parseUriWithPathEndingWithSlash() {
        assertThatThrownBy(() -> WrappingKey.parse("CreateSoftKeyTest", "http://www.banana.com/abc/", OCT))
                .isInstanceOf(IllegalArgumentException.class).hasMessage("keyId path '/abc/' must not end with '/'");
    }

}