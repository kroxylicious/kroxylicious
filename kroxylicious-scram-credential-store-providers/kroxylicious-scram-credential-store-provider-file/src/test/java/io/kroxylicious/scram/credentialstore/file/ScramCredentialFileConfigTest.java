/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.scram.credentialstore.file;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ScramCredentialFileConfigTest {

    private static final InlinePassword DUMMY_PASSWORD = new InlinePassword("secret");

    @Test
    void shouldRejectNullFile() {
        assertThatThrownBy(() -> new ScramCredentialFileConfig(null, DUMMY_PASSWORD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file must not be null");
    }

    @Test
    void shouldRejectEmptyFile() {
        assertThatThrownBy(() -> new ScramCredentialFileConfig("", DUMMY_PASSWORD))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("file must not be empty");
    }

    @Test
    void shouldRejectNullStorePassword() {
        assertThatThrownBy(() -> new ScramCredentialFileConfig("keystore.p12", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("storePassword must not be null");
    }
}
