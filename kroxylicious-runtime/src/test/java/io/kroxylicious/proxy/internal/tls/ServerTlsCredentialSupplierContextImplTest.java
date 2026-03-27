/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.security.cert.X509Certificate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import io.kroxylicious.proxy.tls.ClientTlsContext;
import io.kroxylicious.proxy.tls.TlsCredentials;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

class ServerTlsCredentialSupplierContextImplTest {

    private static TestCertificateUtil.KeyAndCert keyAndCert;

    @BeforeAll
    static void generateCerts() throws Exception {
        keyAndCert = TestCertificateUtil.generateKeyStoreAndCert();
    }

    @Test
    void clientTlsContextPresentWhenProvided() {
        ClientTlsContext client = mock(ClientTlsContext.class);
        var ctx = new ServerTlsCredentialSupplierContextImpl(client);
        assertThat(ctx.clientTlsContext()).isPresent().containsSame(client);
    }

    @Test
    void clientTlsContextEmptyWhenNull() {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        assertThat(ctx.clientTlsContext()).isEmpty();
    }

    @Test
    void tlsCredentialsRejectsNullKey() {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        X509Certificate[] chain = { mock(X509Certificate.class) };
        assertThatThrownBy(() -> ctx.tlsCredentials(null, chain))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("key");
    }

    @Test
    void tlsCredentialsRejectsNullChain() {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        assertThatThrownBy(() -> ctx.tlsCredentials(keyAndCert.privateKey(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("certificateChain");
    }

    @Test
    void tlsCredentialsRejectsEmptyChain() {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        assertThatThrownBy(() -> ctx.tlsCredentials(keyAndCert.privateKey(), new X509Certificate[0]))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must not be empty");
    }

    @Test
    void tlsCredentialsReturnsTlsCredentialsWithValidInput() {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        TlsCredentials creds = ctx.tlsCredentials(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
        assertThat(creds).isInstanceOf(TlsCredentialsImpl.class);
        TlsCredentialsImpl impl = (TlsCredentialsImpl) creds;
        assertThat(impl.privateKey()).isSameAs(keyAndCert.privateKey());
        assertThat(impl.certificateChain()).containsExactly(keyAndCert.cert());
    }

    @Test
    void tlsCredentialsRejectsKeyMismatch() throws Exception {
        var ctx = new ServerTlsCredentialSupplierContextImpl(null);
        TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");

        assertThatThrownBy(() -> ctx.tlsCredentials(other.privateKey(), new X509Certificate[]{ keyAndCert.cert() }))
                .isInstanceOf(BadTlsCredentialsException.class)
                .hasMessageContaining("does not match");
    }
}
