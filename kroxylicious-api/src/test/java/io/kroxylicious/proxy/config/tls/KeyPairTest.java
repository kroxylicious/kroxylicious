/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KeyPairTest {

    @Test
    void testAccept() {
        KeyProvider keyProvider = new KeyPair("/tmp/key", "/tmp/cert", null);
        KeyPair result = keyProvider.accept(new KeyProviderVisitor<>() {

            @Override
            public KeyPair visit(KeyPair keyPair) {
                return keyPair;
            }

            @Override
            public KeyPair visit(KeyStore keyStore) {
                throw new RuntimeException("unexpected call to visit(KeyStore)");
            }

            @Override
            public KeyPair visit(KeyPairSet keyPairSet) {
                throw new RuntimeException("unexpected call to visit(KeyPairSet)");
            }
        });
        assertThat(result).isSameAs(keyProvider);
    }

}