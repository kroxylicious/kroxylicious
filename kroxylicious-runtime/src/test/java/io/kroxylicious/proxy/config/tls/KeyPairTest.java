/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyException;
import java.security.cert.CertificateException;

import javax.crypto.BadPaddingException;

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.proxy.config.tls.TlsTestConstants.BADPASS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.NOT_EXIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class KeyPairTest {

    @Test
    void serverKeyPair() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), null);
        var sslContext = keyPair.forServer().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isServer()).isTrue();
    }

    @Test
    void serverKeyPairKeyProtectedWithPassword() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), new InlinePassword("keypass"));

        var sslContext = keyPair.forServer().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isServer()).isTrue();
    }

    @Test
    void serverKeyPairIncorrectKeyPassword() {
        doFailingKeyPairTest(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), BADPASS)
                .hasRootCauseInstanceOf(BadPaddingException.class)
                .hasMessageContaining("server.crt")
                .hasMessageContaining("server_encrypted.key");
    }

    @Test
    void serverKeyPairCertificateNotFound() {
        doFailingKeyPairTest(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), NOT_EXIST, null)
                .hasRootCauseInstanceOf(CertificateException.class)
                .hasMessageContaining(NOT_EXIST);
    }

    @Test
    void serverKeyPairKeyNotFound() {
        doFailingKeyPairTest(NOT_EXIST, TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), null)
                .hasRootCauseInstanceOf(KeyException.class)
                .hasMessageContaining(NOT_EXIST);
    }

    private AbstractThrowableAssert<?, ? extends Throwable> doFailingKeyPairTest(String serverPrivateKeyFile, String serverCertificateFile,
                                                                                 PasswordProvider keyPassword) {
        var keyPair = new KeyPair(serverPrivateKeyFile, serverCertificateFile, keyPassword);
        return assertThatCode(keyPair::forServer)
                .hasMessageContaining("Error building SSLContext");
    }
}
