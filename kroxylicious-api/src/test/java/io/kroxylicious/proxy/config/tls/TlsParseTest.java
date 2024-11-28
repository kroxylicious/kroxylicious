/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.config.tls;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.MismatchedInputException;
import com.fasterxml.jackson.databind.exc.ValueInstantiationException;

import io.kroxylicious.proxy.config.secret.FilePassword;
import io.kroxylicious.proxy.config.secret.InlinePassword;

import static org.assertj.core.api.Assertions.assertThat;

class TlsParseTest {
    ObjectMapper mapper = new ObjectMapper();

    @Test
    void testEmptyTls() throws Exception {
        String json = """
                {}
                """;
        Tls tls = readTls(json);
        assertThat(tls.trust()).isNull();
        assertThat(tls.key()).isNull();
    }

    @Test
    void testInsecureTlsTrust() throws Exception {
        String json = """
                {
                    "trust": {
                        "insecure": true
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new InsecureTls(true)));
    }

    @Test
    void testTrustStoreStoreFileRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "trust": {
                            "storePassword": {
                                "password": "changeit"
                            }
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("storeFile");
    }

    @Test
    void testTrustStoreStoreFileShouldBeNonNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "trust": {
                            "storeFile": null,
                            "storePassword": {
                                "password": "changeit"
                            }
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testTrustStoreStorePasswordNullAllowed() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file",
                        "storePassword": null
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", null, null)));
    }

    @Test
    void testTrustStoreStorePasswordNotRequired() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file"
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", null, null)));
    }

    @Test
    void testTrustStoreStoreFileInlinePasswordDoesNotAllowNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                       {
                       "trust": {
                            "storeFile": "/tmp/file",
                            "storePassword": {
                                "password": null
                            }
                        }
                    }
                       """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testTrustStoreStoreFileInlinePassword() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file",
                        "storePassword": {
                            "password": "changeit"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", new InlinePassword("changeit"), null)));
    }

    @Test
    void testTrustStoreStoreFileFilePasswordDoesNotAllowNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "trust": {
                            "storeFile": "/tmp/file",
                            "storePassword": {
                                "passwordFile": null
                            }
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testTrustStoreStoreFilePathPasswordDoesNotAllowNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "trust": {
                            "storeFile": "/tmp/file",
                            "storePassword": {
                                "filePath": null
                            }
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testTrustStoreStoreFileFilePassword() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file",
                        "storePassword": {
                            "passwordFile": "/tmp/pass"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null)));
    }

    @Test
    void testTrustStoreStoreFilePathPassword() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file",
                        "storePassword": {
                            "filePath": "/tmp/pass"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), null)));
    }

    @Test
    void testTrustStoreStoreFileType() throws IOException {
        String json = """
                {
                    "trust": {
                        "storeFile": "/tmp/file",
                        "storePassword": {
                            "passwordFile": "/tmp/pass"
                        },
                        "storeType": "PKCS12"
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, new TrustStore("/tmp/file", new FilePassword("/tmp/pass"), "PKCS12")));
    }

    @Test
    void testKeyPair() throws IOException {
        String json = """
                {
                    "key": {
                        "privateKeyFile": "/tmp/key",
                        "certificateFile": "/tmp/cert"
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyPair("/tmp/key", "/tmp/cert", null), null));
    }

    @Test
    void testKeyPairPrivateKeyRequired() {

        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "key": {
                            "certificateFile": "/tmp/cert"
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("privateKeyFile");
    }

    @Test
    void testKeyPairPrivateKeyShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "key": {
                            "privateKeyFile": null,
                            "certificateFile": "/tmp/cert"
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testKeyPairCertificateFileRequired() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "key": {
                            "privateKeyFile": "/tmp/key"
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(MismatchedInputException.class).hasMessageContaining("certificateFile");
    }

    @Test
    void testKeyPairCertificateFileShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "key": {
                            "privateKeyFile": "/tmp/key",
                            "certificateFile": null
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testKeyPairInlinePassword() throws IOException {
        String json = """
                {
                    "key": {
                        "privateKeyFile": "/tmp/key",
                        "certificateFile": "/tmp/cert",
                        "keyPassword": {
                            "password": "changeit"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyPair("/tmp/key", "/tmp/cert", new InlinePassword("changeit")), null));
    }

    @Test
    void testKeyPairFilePathPassword() throws IOException {
        String json = """
                {
                    "key": {
                        "privateKeyFile": "/tmp/key",
                        "certificateFile": "/tmp/cert",
                        "keyPassword": {
                            "passwordFile": "/tmp/pass"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyPair("/tmp/key", "/tmp/cert", new FilePassword("/tmp/pass")), null));
    }

    @Test
    void testKeyStore() throws IOException {
        String json = """
                {
                    "key": {
                        "storeFile": "/tmp/store"
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyStore("/tmp/store", null, null, null), null));
    }

    @Test
    void testKeyStoreInlineStorePasswordProvider() throws IOException {
        String json = """
                {
                    "key": {
                        "storeFile": "/tmp/store",
                        "storePassword": {
                            "password": "changeit"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyStore("/tmp/store", new InlinePassword("changeit"), null, null), null));
    }

    @Test
    void testKeyStoreInlineKeyPasswordProvider() throws IOException {
        String json = """
                {
                    "key": {
                        "storeFile": "/tmp/store",
                        "keyPassword": {
                            "password": "changeit"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyStore("/tmp/store", null, new InlinePassword("changeit"), null), null));
    }

    @Test
    void testKeyStoreFilePathKeyPasswordProvider() throws IOException {
        String json = """
                {
                    "key": {
                        "storeFile": "/tmp/store",
                        "keyPassword": {
                            "passwordFile": "/tmp/pass"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyStore("/tmp/store", null, new FilePassword("/tmp/pass"), null), null));
    }

    @Test
    void testKeyStoreStoreFileShouldNotBeNull() {
        Assertions.assertThatThrownBy(() -> {
            String json = """
                    {
                        "key": {
                            "storeFile": null,
                            "keyPassword": {
                                "passwordFile": "/tmp/pass"
                            }
                        }
                    }
                    """;
            readTls(json);
        }).isInstanceOf(ValueInstantiationException.class).cause().isInstanceOf(NullPointerException.class);
    }

    @Test
    void testKeyStoreFilePathStorePasswordProvider() throws IOException {
        String json = """
                {
                    "key": {
                        "storeFile": "/tmp/store",
                        "storePassword": {
                            "passwordFile": "/tmp/pass"
                        }
                    }
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(new KeyStore("/tmp/store", new FilePassword("/tmp/pass"), null, null), null));
    }

    @Test
    void testClientAuthSettingsParsed() throws IOException {
        String json = """
                {
                    "clientAuth": "NONE"
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, null, TlsClientAuth.NONE));
    }

    @Test
    void shouldAcceptNullClientAuth() throws IOException {
        String json = """
                {
                    "clientAuth": null
                }
                """;
        Tls tls = readTls(json);
        assertThat(tls).isEqualTo(new Tls(null, null, TlsClientAuth.NONE));
    }

    private Tls readTls(String json) throws IOException {
        return mapper.reader().readValue(json, Tls.class);
    }
}
