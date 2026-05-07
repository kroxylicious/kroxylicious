/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.certificate;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;

import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PemParserTest {

    private static KeyPair keyPair;
    private static X509Certificate certificate;
    private static byte[] pkcs8KeyPem;
    private static byte[] pkcs1KeyPem;
    private static byte[] certPem;

    @BeforeAll
    static void setUp() throws Exception {
        keyPair = CertificateGenerator.generateRsaKeyPair();
        certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);

        pkcs1KeyPem = Files.readAllBytes(CertificateGenerator.writeRsaPrivateKeyPem(keyPair));
        certPem = Files.readAllBytes(CertificateGenerator.generateCertPem(certificate));
        pkcs8KeyPem = writePkcs8Pem(keyPair.getPrivate());
    }

    private static byte[] writePkcs8Pem(PrivateKey key) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
            writer.writeObject(new JcaPKCS8Generator(key, null));
        }
        return sw.toString().getBytes(java.nio.charset.StandardCharsets.US_ASCII);
    }

    @Nested
    class ParsePrivateKey {

        @Test
        void parsesPkcs8RsaKey() throws Exception {
            PrivateKey parsed = PemParser.parsePrivateKey(pkcs8KeyPem);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(keyPair.getPrivate().getEncoded());
        }

        @Test
        void parsesPkcs1RsaKey() throws Exception {
            PrivateKey parsed = PemParser.parsePrivateKey(pkcs1KeyPem);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(keyPair.getPrivate().getEncoded());
        }

        @Test
        void rejectsEmptyPemData() {
            assertThatThrownBy(() -> PemParser.parsePrivateKey("".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No private key found");
        }

        @Test
        void rejectsGarbageData() {
            assertThatThrownBy(() -> PemParser.parsePrivateKey("not a pem file".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No private key found");
        }

        @Test
        void rejectsInvalidKeyData() {
            String pem = "-----BEGIN PRIVATE KEY-----\nYWJj\n-----END PRIVATE KEY-----\n";
            assertThatThrownBy(() -> PemParser.parsePrivateKey(pem.getBytes()))
                    .isInstanceOf(IOException.class);
        }

        @Test
        void handlesWhitespaceInPemBody() throws Exception {
            String pemWithExtraSpace = "-----BEGIN PRIVATE KEY-----\n\n  "
                    + Base64.getMimeEncoder(64, "\n".getBytes())
                            .encodeToString(keyPair.getPrivate().getEncoded())
                    + "\n  \n-----END PRIVATE KEY-----\n";
            PrivateKey parsed = PemParser.parsePrivateKey(pemWithExtraSpace.getBytes());
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
        }
    }

    @Nested
    class ParseCertificateChain {

        @Test
        void parsesSingleCertificate() throws Exception {
            X509Certificate[] chain = PemParser.parseCertificateChain(certPem);
            assertThat(chain).hasSize(1);
            assertThat(chain[0].getSubjectX500Principal()).isEqualTo(certificate.getSubjectX500Principal());
        }

        @Test
        void parsesMultipleCertificates() throws Exception {
            KeyPair otherPair = CertificateGenerator.generateRsaKeyPair();
            X509Certificate otherCert = CertificateGenerator.generateSelfSignedX509Certificate(otherPair);
            byte[] otherCertPem = Files.readAllBytes(CertificateGenerator.generateCertPem(otherCert));

            byte[] multiPem = new byte[certPem.length + otherCertPem.length];
            System.arraycopy(certPem, 0, multiPem, 0, certPem.length);
            System.arraycopy(otherCertPem, 0, multiPem, certPem.length, otherCertPem.length);

            X509Certificate[] chain = PemParser.parseCertificateChain(multiPem);
            assertThat(chain).hasSize(2);
        }

        @Test
        void rejectsEmptyData() {
            assertThatThrownBy(() -> PemParser.parseCertificateChain("".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No certificates found");
        }

        @Test
        void rejectsGarbageData() {
            assertThatThrownBy(() -> PemParser.parseCertificateChain("not a cert".getBytes()))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No certificates found");
        }
    }
}
