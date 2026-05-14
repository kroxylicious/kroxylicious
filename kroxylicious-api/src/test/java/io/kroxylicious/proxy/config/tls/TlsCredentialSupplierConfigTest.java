/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TlsCredentialSupplierConfigTest {

    @Test
    void shouldCreateValidConfigWithTypeOnly() {
        // Given/When
        var config = new TlsCredentialSupplierConfig("MySupplier", null);

        // Then
        assertThat(config.type()).isEqualTo("MySupplier");
        assertThat(config.config()).isNull();
    }

    @Test
    void shouldCreateValidConfigWithTypeAndConfig() {
        // Given
        var configValue = new Object();

        // When
        var config = new TlsCredentialSupplierConfig("MySupplier", configValue);

        // Then
        assertThat(config.type()).isEqualTo("MySupplier");
        assertThat(config.config()).isSameAs(configValue);
    }

    @Test
    void shouldRejectNullType() {
        // Given/When/Then
        assertThatThrownBy(() -> new TlsCredentialSupplierConfig(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("TLS credential supplier type must not be null");
    }
}
