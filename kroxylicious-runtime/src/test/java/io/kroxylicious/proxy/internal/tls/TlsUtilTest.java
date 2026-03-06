/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import io.netty.handler.ssl.SslContextBuilder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TlsUtilTest {

    private static TestCertificateUtil.KeyAndCert keyAndCert;
    private static String certPem;
    private static String keyPem;

    @BeforeAll
    static void setUp() throws Exception {
        keyAndCert = TestCertificateUtil.generateKeyStoreAndCert();
        certPem = TestCertificateUtil.toPem(keyAndCert.cert());
        keyPem = TestCertificateUtil.toPkcs8Pem(keyAndCert.privateKey());
    }

    @Nested
    class ParsePrivateKey {

        @Test
        void parsesPkcs8RsaKey() throws Exception {
            PrivateKey parsed = TlsUtil.parsePrivateKey(keyPem.getBytes());
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(keyAndCert.privateKey().getEncoded());
        }

        @Test
        void rejectsEmptyPemData() {
            assertThatThrownBy(() -> TlsUtil.parsePrivateKey("".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No private key found");
        }

        @Test
        void rejectsGarbageData() {
            assertThatThrownBy(() -> TlsUtil.parsePrivateKey("not a pem file".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No private key found");
        }

        @Test
        void rejectsMissingEndMarker() {
            String broken = "-----BEGIN PRIVATE KEY-----\nMIIE==\n";
            assertThatThrownBy(() -> TlsUtil.parsePrivateKey(broken.getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No matching END marker");
        }

        @Test
        void rejectsInvalidBase64InPkcs8() {
            String pem = "-----BEGIN PRIVATE KEY-----\n!!invalid!!\n-----END PRIVATE KEY-----\n";
            assertThatThrownBy(() -> TlsUtil.parsePrivateKey(pem.getBytes()))
                    .isInstanceOf(Exception.class);
        }

        @Test
        void rejectsInvalidKeyData() {
            String pem = "-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----\n";
            assertThatThrownBy(() -> TlsUtil.parsePrivateKey(pem.getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Failed to parse private key");
        }

        @Test
        void parsesPkcs1RsaKey() throws Exception {
            String pkcs1Pem = TestCertificateUtil.toPkcs1Pem(keyAndCert.privateKey());
            PrivateKey parsed = TlsUtil.parsePrivateKey(pkcs1Pem.getBytes());
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            // The parsed key should produce the same encoded form as the original
            assertThat(parsed.getEncoded()).isEqualTo(keyAndCert.privateKey().getEncoded());
        }

        @Test
        void handlesWhitespaceInPemBody() throws Exception {
            // Add extra whitespace/newlines - should still parse
            String pemWithExtraSpace = "-----BEGIN PRIVATE KEY-----\n\n  " +
                    java.util.Base64.getMimeEncoder(64, "\n".getBytes())
                            .encodeToString(keyAndCert.privateKey().getEncoded())
                    +
                    "\n  \n-----END PRIVATE KEY-----\n";
            PrivateKey parsed = TlsUtil.parsePrivateKey(pemWithExtraSpace.getBytes());
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
        }
    }

    @Nested
    class ParseCertificateChain {

        @Test
        void parsesSingleCertificate() throws Exception {
            X509Certificate[] chain = TlsUtil.parseCertificateChain(certPem.getBytes());
            assertThat(chain).hasSize(1);
            assertThat(chain[0].getSubjectX500Principal()).isEqualTo(keyAndCert.cert().getSubjectX500Principal());
        }

        @Test
        void parsesMultipleCertificates() throws Exception {
            TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            String multiPem = certPem + TestCertificateUtil.toPem(other.cert());

            X509Certificate[] chain = TlsUtil.parseCertificateChain(multiPem.getBytes());
            assertThat(chain).hasSize(2);
        }

        @Test
        void rejectsEmptyData() {
            assertThatThrownBy(() -> TlsUtil.parseCertificateChain("".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No certificates found");
        }

        @Test
        void rejectsGarbageData() {
            assertThatThrownBy(() -> TlsUtil.parseCertificateChain("not a cert".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No certificates found");
        }
    }

    @Nested
    class ValidateKeyAndCertMatch {

        @Test
        void acceptsMatchingKeyAndCert() {
            TlsUtil.validateKeyAndCertMatch(keyAndCert.privateKey(), keyAndCert.cert());
        }

        @Test
        void rejectsMismatchedKeyAndCert() throws Exception {
            TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(other.privateKey(), keyAndCert.cert()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not match");
        }

        @Test
        void rejectsAlgorithmMismatch() {
            PrivateKey mockKey = mock(PrivateKey.class);
            when(mockKey.getAlgorithm()).thenReturn("DSA");
            assertThatThrownBy(() -> TlsUtil.validateKeyAndCertMatch(mockKey, keyAndCert.cert()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not match certificate public key algorithm");
        }
    }

    @Nested
    class ValidateCertificateChain {

        @Test
        void acceptsSingleSelfSignedCert() {
            TlsUtil.validateCertificateChain(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
        }

        @Test
        void rejectsEmptyChain() {
            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(keyAndCert.privateKey(), new X509Certificate[0]))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("empty");
        }

        @Test
        void rejectsKeyMismatch() throws Exception {
            TestCertificateUtil.KeyAndCert other = TestCertificateUtil.generateKeyStoreAndCert("CN=other");
            assertThatThrownBy(() -> TlsUtil.validateCertificateChain(other.privateKey(), new X509Certificate[]{ keyAndCert.cert() }))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("does not match");
        }
    }

    @Nested
    class ToClientSslContext {

        @Test
        void buildsSslContextFromCredentials() throws Exception {
            TlsCredentialsImpl creds = new TlsCredentialsImpl(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
            var sslContext = TlsUtil.toClientSslContext(creds);
            assertThat(sslContext).isNotNull();
            assertThat(sslContext.isClient()).isTrue();
        }

        @Test
        void buildsSslContextWithCustomBuilder() throws Exception {
            TlsCredentialsImpl creds = new TlsCredentialsImpl(keyAndCert.privateKey(), new X509Certificate[]{ keyAndCert.cert() });
            SslContextBuilder builder = SslContextBuilder.forClient();
            var sslContext = TlsUtil.toClientSslContext(creds, builder);
            assertThat(sslContext).isNotNull();
            assertThat(sslContext.isClient()).isTrue();
        }
    }
}
