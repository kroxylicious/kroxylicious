/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.config.secret.InlinePassword;

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
        Tls tls = new Tls(new KeyPair("/tmp/key", "/tmp/cert", null), null, null, null);
        assertThat(tls.definesKey()).isTrue();
    }

    @Test
    void testKeyNotDefined() {
        Tls tls = new Tls(null, null, null, null);
        assertThat(tls.definesKey()).isFalse();
    }

    @Test
    void testTrustDefined() {
        Tls tls = new Tls(null, new TrustStore("/tmp/certs", new InlinePassword("pass"), null), null, null);
        assertThat(tls.trust()).isNotNull();
        assertThat(tls.definesKey()).isFalse();
    }

    @Test
    void testCipherSuitesDefined() {
        Tls tls = new Tls(null, null, new AllowDeny<String>(List.of("ALLOWED_CIPHER_SUITES"), null), null);
        assertThat(tls.cipherSuites()).isNotNull();
    }

    @Test
    void testEnabledProtocolsDefined() {
        Tls tls = new Tls(null, null, null, new AllowDeny<>(List.of("TLSv1.2"), null));
        assertThat(tls.protocols()).isNotNull();
    }

}
