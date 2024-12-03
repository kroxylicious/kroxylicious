/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyStore;
import java.util.Locale;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TrustStoreTest {

    @Test
    void testTypeDefaultsToPlatformDefault() {
        TrustStore store = new TrustStore("/tmp/store", null, null);
        assertThat(store.getType()).isEqualTo(KeyStore.getDefaultType().toUpperCase(Locale.ROOT));
        assertThat(store.isPemType()).isFalse();
    }

    @Test
    void testSpecifyingStoreType() {
        TrustStore store = new TrustStore("/tmp/store", null, "PKCS12");
        assertThat(store.getType()).isEqualTo("PKCS12");
        assertThat(store.isPemType()).isFalse();
    }

    @Test
    void testPemType() {
        TrustStore store = new TrustStore("/tmp/store", null, "PEM");
        assertThat(store.getType()).isEqualTo("PEM");
        assertThat(store.isPemType()).isTrue();
    }

    @Test
    void testClientAuth() {
        TrustStore store = new TrustStore("/tmp/store", null, "PKCS12", TlsClientAuth.REQUIRED);
        assertThat(store.clientAuth()).isEqualTo(TlsClientAuth.REQUIRED);
    }

    @Test
    void testClientAuthPermitsNull() {
        TrustStore store = new TrustStore("/tmp/store", null, "PKCS12", null);
        assertThat(store.clientAuth()).isNull();
    }

    @Test
    void testAccept() {
        TrustProvider trustProvider = new TrustStore("/tmp/store", null, "PEM", null);
        TrustStore result = trustProvider.accept(new TrustProviderVisitor<>() {
            @Override
            public TrustStore visit(TrustStore trustStore) {
                return trustStore;
            }

            @Override
            public TrustStore visit(InsecureTls insecureTls) {
                throw new RuntimeException("unexpected call to visit(InsecureTls)");
            }

            @Override
            public TrustStore visit(PlatformTrustProvider platformTrustProviderTls) {
                throw new RuntimeException("unexpected call to visit(PlatformTrustProvider)");
            }
        });
        assertThat(result).isSameAs(trustProvider);
    }

}
