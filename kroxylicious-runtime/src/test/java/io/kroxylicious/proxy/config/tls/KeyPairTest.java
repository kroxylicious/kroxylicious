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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

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

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void keyPairIncorrectKeyPassword(boolean forServer) {
        doFailingKeyPairTest(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), BADPASS, forServer)
                .hasRootCauseInstanceOf(BadPaddingException.class)
                .hasMessageContaining("server.crt")
                .hasMessageContaining("server_encrypted.key");
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void keyPairCertificateNotFound(boolean forServer) {
        doFailingKeyPairTest(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), NOT_EXIST, null, forServer)
                .hasRootCauseInstanceOf(CertificateException.class)
                .hasMessageContaining(NOT_EXIST);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void keyPairKeyNotFound(boolean forServer) {
        doFailingKeyPairTest(NOT_EXIST, TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), null, forServer)
                .hasRootCauseInstanceOf(KeyException.class)
                .hasMessageContaining(NOT_EXIST);
    }

    @Test
    void clientKeyPair() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), null);
        var sslContext = keyPair.forClient().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isClient()).isTrue();
    }

    @Test
    void clientKeyPairKeyProtectedWithPassword() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), new InlinePassword("keypass"));

        var sslContext = keyPair.forClient().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isClient()).isTrue();
    }

    private AbstractThrowableAssert<?, ? extends Throwable> doFailingKeyPairTest(String privateKeyFile, String certificateFile,
                                                                                 PasswordProvider keyPassword, boolean forServer) {
        var keyPair = new KeyPair(privateKeyFile, certificateFile, keyPassword);
        return assertThatCode(forServer ? keyPair::forServer : keyPair::forClient)
                .hasMessageContaining("Error building SSLContext");
    }
}