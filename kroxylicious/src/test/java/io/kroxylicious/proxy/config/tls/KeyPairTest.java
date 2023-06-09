/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.security.KeyException;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;

import static io.kroxylicious.proxy.config.tls.TlsTestConstants.BADPASS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.NOT_EXIST;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class KeyPairTest {

    @Test
    public void serverKeyPair() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), null);
        var sslContext = keyPair.forServer().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isServer()).isTrue();
    }

    @Test
    public void serverKeyPairKeyProtectedWithPassword() throws Exception {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), new InlinePassword("keypass"));

        var sslContext = keyPair.forServer().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isServer()).isTrue();
    }

    @Test
    public void serverKeyPairIncorrectKeyPassword() {
        var keyPair = new KeyPair(TlsTestConstants.getResourceLocationOnFilesystem("server_encrypted.key"),
                TlsTestConstants.getResourceLocationOnFilesystem("server.crt"), BADPASS);
        assertThatCode(keyPair::forServer).hasCauseInstanceOf(InvalidKeySpecException.class);
    }

    @Test
    public void serverKeyPairCertificateNotFound() {
        serverKeyPairNotFound(TlsTestConstants.getResourceLocationOnFilesystem("server.key"), NOT_EXIST).hasCauseInstanceOf(CertificateException.class)
                .hasStackTraceContaining(NOT_EXIST);
    }

    @Test
    public void serverKeyPairKeyNotFound() {
        serverKeyPairNotFound(NOT_EXIST, TlsTestConstants.getResourceLocationOnFilesystem("server.crt")).hasCauseInstanceOf(KeyException.class)
                .hasStackTraceContaining(NOT_EXIST);
    }

    private AbstractThrowableAssert<?, ? extends Throwable> serverKeyPairNotFound(String serverPrivateKeyFile, String serverCertificateFile) {
        var keyPair = new KeyPair(serverPrivateKeyFile, serverCertificateFile, null);
        return assertThatCode(keyPair::forServer);
    }
}