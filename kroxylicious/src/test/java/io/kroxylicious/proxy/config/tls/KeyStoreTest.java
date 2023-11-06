/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.IOException;
import java.security.UnrecoverableKeyException;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static io.kroxylicious.proxy.config.tls.TlsTestConstants.BADPASS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.JKS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.KEYPASS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.KEYPASS_FILE_PASSWORD;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.KEYSTORE_FILE_PASSWORD;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.NOT_EXIST;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.PEM;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.PKCS_12;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.STOREPASS;
import static io.kroxylicious.proxy.config.tls.TlsTestConstants.getResourceLocationOnFilesystem;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class KeyStoreTest {

    public static Stream<Arguments> serverWithKeyStore() {
        return Stream.of(
                Arguments.of("Platform Default Store JKS", null, "server.jks", STOREPASS, null),
                Arguments.of("JKS store=key", JKS, "server.jks", STOREPASS, null),
                Arguments.of("JKS store=key explicit", JKS, "server.jks", STOREPASS, STOREPASS),
                Arguments.of("JKS store!=key", JKS, "server_diff_keypass.jks", STOREPASS, KEYPASS),
                Arguments.of("PKCS12", PKCS_12, "server.p12", STOREPASS, null),
                Arguments.of("Combined key/crt PEM passed as keyStore (KIP-651)", PEM, "server_key_crt.pem", null, null),
                Arguments.of("Combined key/crt PEM passed as keyStore (KIP-651) with encrypted key", PEM, "server_crt_encrypted_key.pem", null, KEYPASS),
                Arguments.of("JKS keystore from file", JKS, "server_diff_keypass.jks", KEYSTORE_FILE_PASSWORD, KEYPASS_FILE_PASSWORD));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource()
    void serverWithKeyStore(String name,
                            String storeType,
                            String storeFile, PasswordProvider storePassword, PasswordProvider keyPassword)
            throws Exception {
        var keyStore = new KeyStore(getResourceLocationOnFilesystem(storeFile), storePassword, keyPassword, storeType);

        var sslContext = keyStore.forServer().build();
        assertThat(sslContext).isNotNull();
        assertThat(sslContext.isServer()).isTrue();
    }

    @Test
    void serverKeyStoreFileNotFound() {
        var keyStore = new KeyStore(NOT_EXIST, null, null, null);

        assertThatCode(keyStore::forServer).hasCauseInstanceOf(IOException.class).hasMessageContaining(NOT_EXIST);
    }

    @Test
    void serverKeyStoreIncorrectPassword() {
        var keyStore = new KeyStore(getResourceLocationOnFilesystem("server.jks"),
                BADPASS,
                null,
                null);

        assertThatCode(keyStore::forServer).hasRootCauseInstanceOf(UnrecoverableKeyException.class);
    }

    @Test
    void serverKeyStoreIncorrectKeyPassword() {
        var keyStore = new KeyStore(getResourceLocationOnFilesystem("server_diff_keypass.jks"),
                STOREPASS,
                BADPASS,
                null);

        assertThatCode(keyStore::forServer).hasRootCauseInstanceOf(UnrecoverableKeyException.class);
    }

}
