/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.util.Locale;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyStoreTest {

    @Test
    void testGetTypeDefaultsToPlatformDefault() {
        KeyStore keyStore = new KeyStore("/tmp/store", null, null, null);
        assertThat(keyStore.getType()).isEqualTo(java.security.KeyStore.getDefaultType().toUpperCase(Locale.ROOT));
        assertThat(keyStore.isPemType()).isFalse();
    }

    @Test
    void testDefinedTypeIsUsed() {
        KeyStore keyStore = new KeyStore("/tmp/store", null, null, "PEM");
        assertThat(keyStore.getType()).isEqualTo("PEM");
    }

    @Test
    void testIsPem() {
        KeyStore keyStore = new KeyStore("/tmp/store", null, null, "PEM");
        assertThat(keyStore.isPemType()).isTrue();
    }

    @Test
    void testIsNotPem() {
        KeyStore keyStore = new KeyStore("/tmp/store", null, null, "JKS");
        assertThat(keyStore.isPemType()).isFalse();
    }

    @Test
    void testAccept() {
        KeyProvider keyProvider = new KeyStore("/tmp/store", null, null, "JKS");
        KeyStore result = keyProvider.accept(new KeyProviderVisitor<>() {

            @Override
            public KeyStore visit(KeyPair keyPair) {
                throw new RuntimeException("unexpected call to visit(KeyPair)");
            }

            @Override
            public KeyStore visit(KeyStore keyStore) {
                return keyStore;
            }

            @Override
            public KeyStore visit(KeyPairSet keyPairSet) {
                throw new RuntimeException("unexpected call to visit(KeyPairSet)");
            }
        });
        assertThat(result).isSameAs(keyProvider);
    }

}
