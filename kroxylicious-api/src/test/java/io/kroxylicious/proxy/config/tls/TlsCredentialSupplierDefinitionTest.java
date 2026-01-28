/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TlsCredentialSupplierDefinitionTest {

    @Test
    void shouldCreateValidDefinitionWithTypeOnly() {
        // Given/When
        var definition = new TlsCredentialSupplierDefinition("MySupplier", null);

        // Then
        assertThat(definition.type()).isEqualTo("MySupplier");
        assertThat(definition.config()).isNull();
    }

    @Test
    void shouldCreateValidDefinitionWithTypeAndConfig() {
        // Given
        var config = new Object();

        // When
        var definition = new TlsCredentialSupplierDefinition("MySupplier", config);

        // Then
        assertThat(definition.type()).isEqualTo("MySupplier");
        assertThat(definition.config()).isSameAs(config);
    }

    @Test
    void shouldRejectNullType() {
        // Given/When/Then
        assertThatThrownBy(() -> new TlsCredentialSupplierDefinition(null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("TLS credential supplier type must not be null");
    }
}
