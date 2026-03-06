/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TlsCredentialsImplTest {

    @Test
    void constructorStoresKeyAndChain() {
        PrivateKey key = mock(PrivateKey.class);
        X509Certificate cert = mock(X509Certificate.class);
        X509Certificate[] chain = { cert };

        TlsCredentialsImpl creds = new TlsCredentialsImpl(key, chain);

        assertThat(creds.getPrivateKey()).isSameAs(key);
        assertThat(creds.getCertificateChain()).containsExactly(cert);
    }

    @Test
    void constructorRejectsNullKey() {
        X509Certificate cert = mock(X509Certificate.class);
        assertThatThrownBy(() -> new TlsCredentialsImpl(null, new X509Certificate[]{ cert }))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("privateKey");
    }

    @Test
    void constructorRejectsNullChain() {
        PrivateKey key = mock(PrivateKey.class);
        assertThatThrownBy(() -> new TlsCredentialsImpl(key, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("certificateChain");
    }

    @Test
    void constructorRejectsEmptyChain() {
        PrivateKey key = mock(PrivateKey.class);
        assertThatThrownBy(() -> new TlsCredentialsImpl(key, new X509Certificate[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void toStringContainsCertCountAndAlgorithm() {
        PrivateKey key = mock(PrivateKey.class);
        when(key.getAlgorithm()).thenReturn("RSA");
        X509Certificate cert = mock(X509Certificate.class);

        TlsCredentialsImpl creds = new TlsCredentialsImpl(key, new X509Certificate[]{ cert });

        assertThat(creds.toString())
                .contains("1 certificates")
                .contains("RSA");
    }

    @Test
    void toStringWithMultipleCerts() {
        PrivateKey key = mock(PrivateKey.class);
        when(key.getAlgorithm()).thenReturn("EC");
        X509Certificate cert1 = mock(X509Certificate.class);
        X509Certificate cert2 = mock(X509Certificate.class);

        TlsCredentialsImpl creds = new TlsCredentialsImpl(key, new X509Certificate[]{ cert1, cert2 });

        assertThat(creds.toString())
                .contains("2 certificates")
                .contains("EC");
    }
}
