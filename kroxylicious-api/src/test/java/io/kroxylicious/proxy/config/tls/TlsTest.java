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
        Tls tls = new Tls(new KeyPair("/tmp/key", "/tmp/cert", null), null);
        assertThat(tls.definesKey()).isTrue();
    }

    @Test
    void testKeyNotDefined() {
        Tls tls = new Tls(null, null);
        assertThat(tls.definesKey()).isFalse();
    }

    @Test
    void testClientAuthDefined() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), TlsClientAuth.NONE);
        assertThat(tls.definesClientAuth()).isTrue();
    }

    @Test
    void testClientAuthNotDefinedNoTrust() {
        Tls tls = new Tls(null, null, TlsClientAuth.REQUIRED);
        assertThat(tls.definesClientAuth()).isFalse();
    }

    @Test
    void testClientAuthNotDefinedNoClientAuth() {
        Tls tls = new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null), null);
        assertThat(tls.definesClientAuth()).isFalse();
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
