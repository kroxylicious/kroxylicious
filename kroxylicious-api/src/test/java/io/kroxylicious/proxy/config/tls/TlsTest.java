/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.secret.FilePassword;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TlsTest {

    @Test
    void testGetStoreTypeOrPlatformDefault() {
        assertThat(Tls.getStoreTypeOrPlatformDefault("PEM")).isEqualTo("PEM");
        assertThat(Tls.getStoreTypeOrPlatformDefault("JKS")).isEqualTo("JKS");
        assertThat(Tls.getStoreTypeOrPlatformDefault("jKs")).isEqualTo("JKS");
        assertThat(Tls.getStoreTypeOrPlatformDefault("jks")).isEqualTo("JKS");
        assertThat(Tls.getStoreTypeOrPlatformDefault(null)).isEqualTo(KeyStore.getDefaultType().toUpperCase(Locale.ROOT));
    }

    @Test
    void testKeyDefined() {
        Tls tls = new Tls(new KeyPair("/tmp/key", "/tmp/cert", null), null, null);
        assertThat(tls.definesKey()).isTrue();
    }

    @Test
    void testKeyNotDefined() {
        Tls tls = new Tls(null, null, null);
        assertThat(tls.definesKey()).isFalse();
    }

    @Test
    void testClientAuthDefined() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), TlsClientAuth.NONE);
        assertThat(tls.validateClientAuth()).isTrue();
    }

    @Test
    void shouldRequireTrustStoreForClientAuthRequired() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new Tls(null, null, TlsClientAuth.REQUIRED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ClientAuth enabled but no TrustStore provided to validate certificates");
    }

    @Test
    void shouldRequireTrustStoreForClientAuthRequested() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new Tls(null, null, TlsClientAuth.REQUESTED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("ClientAuth enabled but no TrustStore provided to validate certificates");
    }

    @Test
    void shouldNotRequireTrustStoreForClientAuthNone() {
        // Given
        Tls tls = new Tls(null, null, TlsClientAuth.NONE);

        // When
        final boolean actual = tls.validateClientAuth();

        // Then
        assertThat(actual).isTrue();
    }

    @Test
    void testClientAuthRequire() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), TlsClientAuth.REQUIRED);
        assertThat(tls.clientAuth()).isEqualTo(TlsClientAuth.REQUIRED);
    }

    @Test
    void testClientAuthOptional() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), TlsClientAuth.REQUESTED);
        assertThat(tls.clientAuth()).isEqualTo(TlsClientAuth.REQUESTED);
    }

    @Test
    void testClientAuthNone() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), TlsClientAuth.NONE);
        assertThat(tls.clientAuth()).isEqualTo(TlsClientAuth.NONE);
    }

    @Test
    void testClientAuthNoTrustNone() {
        Tls tls = new Tls(null, null, null);
        assertThat(tls.clientAuth()).isEqualTo(TlsClientAuth.NONE);
    }

}
