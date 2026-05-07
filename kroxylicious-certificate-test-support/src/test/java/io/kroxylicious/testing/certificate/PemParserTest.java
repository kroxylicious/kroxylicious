/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.certificate;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.security.spec.ECGenParameterSpec;
import java.util.Base64;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PemParserTest {

    private static KeyPair rsaKeyPair;
    private static X509Certificate certificate;
    private static byte[] pkcs8KeyPem;
    private static byte[] pkcs1KeyPem;
    private static byte[] certPem;

    @BeforeAll
    static void setUp() throws Exception {
        rsaKeyPair = CertificateGenerator.generateRsaKeyPair();
        certificate = CertificateGenerator.generateSelfSignedX509Certificate(rsaKeyPair);

        pkcs1KeyPem = Files.readAllBytes(CertificateGenerator.writeRsaPrivateKeyPem(rsaKeyPair));
        certPem = Files.readAllBytes(CertificateGenerator.generateCertPem(certificate));
        pkcs8KeyPem = writePkcs8Pem(rsaKeyPair.getPrivate());
    }

    private static byte[] writePkcs8Pem(PrivateKey key) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
            writer.writeObject(new JcaPKCS8Generator(key, null));
        }
        return sw.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] writeEncryptedPkcs8Pem(PrivateKey key, char[] password) throws Exception {
        var encryptorBuilder = new JceOpenSSLPKCS8EncryptorBuilder(org.bouncycastle.asn1.nist.NISTObjectIdentifiers.id_aes256_CBC);
        encryptorBuilder.setProvider(new BouncyCastleProvider());
        encryptorBuilder.setPassword(password);
        encryptorBuilder.setRandom(new SecureRandom());
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
            writer.writeObject(new JcaPKCS8Generator(key, encryptorBuilder.build()));
        }
        return sw.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static byte[] writeTraditionalEncryptedPem(KeyPair pair, char[] password) throws IOException {
        StringWriter sw = new StringWriter();
        try (JcaPEMWriter writer = new JcaPEMWriter(sw)) {
            writer.writeObject(pair.getPrivate(), new JcePEMEncryptorBuilder("AES-128-CBC").setProvider(new BouncyCastleProvider()).build(password));
        }
        return sw.toString().getBytes(StandardCharsets.US_ASCII);
    }

    private static KeyPair generateEcKeyPair() throws Exception {
        KeyPairGenerator gen = KeyPairGenerator.getInstance("EC");
        gen.initialize(new ECGenParameterSpec("secp256r1"), new SecureRandom());
        return gen.generateKeyPair();
    }

    @Nested
    class ParsePrivateKey {

        @Test
        void parsesPkcs8RsaKey() throws Exception {
            PrivateKey parsed = PemParser.parsePrivateKey(pkcs8KeyPem);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(rsaKeyPair.getPrivate().getEncoded());
        }

        @Test
        void parsesPkcs1RsaKey() throws Exception {
            PrivateKey parsed = PemParser.parsePrivateKey(pkcs1KeyPem);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(rsaKeyPair.getPrivate().getEncoded());
        }

        @Test
        void parsesEcKey() throws Exception {
            KeyPair ecPair = generateEcKeyPair();
            byte[] ecPem = writePkcs8Pem(ecPair.getPrivate());
            PrivateKey parsed = PemParser.parsePrivateKey(ecPem);
            assertThat(parsed.getAlgorithm()).isEqualTo("EC");
            assertThat(parsed.getEncoded()).isEqualTo(ecPair.getPrivate().getEncoded());
        }

        @Test
        void parsesEncryptedPkcs8Key() throws Exception {
            char[] password = "test-password".toCharArray();
            byte[] encryptedPem = writeEncryptedPkcs8Pem(rsaKeyPair.getPrivate(), password);
            PrivateKey parsed = PemParser.parsePrivateKey(encryptedPem, password);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(rsaKeyPair.getPrivate().getEncoded());
        }

        @Test
        void parsesTraditionalEncryptedKey() throws Exception {
            char[] password = "test-password".toCharArray();
            byte[] encryptedPem = writeTraditionalEncryptedPem(rsaKeyPair, password);
            PrivateKey parsed = PemParser.parsePrivateKey(encryptedPem, password);
            assertThat(parsed.getAlgorithm()).isEqualTo("RSA");
            assertThat(parsed.getEncoded()).isEqualTo(rsaKeyPair.getPrivate().getEncoded());
        }

        @Test
        void rejectsEncryptedPkcs8KeyWithoutPassword() throws Exception {
            char[] password = "test-password".toCharArray();
            byte[] encryptedPem = writeEncryptedPkcs8Pem(rsaKeyPair.getPrivate(), password);
            assertThatThrownBy(() -> PemParser.parsePrivateKey(encryptedPem))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Encrypted private key requires a password");
        }

        @Test
        void rejectsTraditionalEncryptedKeyWithoutPassword() throws Exception {
            char[] password = "test-password".toCharArray();
            byte[] encryptedPem = writeTraditionalEncryptedPem(rsaKeyPair, password);
            assertThatThrownBy(() -> PemParser.parsePrivateKey(encryptedPem))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Encrypted private key requires a password");
        }

        @Test
        void rejectsEncryptedKeyWithWrongPassword() throws Exception {
            char[] password = "correct-password".toCharArray();
            byte[] encryptedPem = writeEncryptedPkcs8Pem(rsaKeyPair.getPrivate(), password);
            assertThatThrownBy(() -> PemParser.parsePrivateKey(encryptedPem, "wrong-password".toCharArray()))
                    .isInstanceOf(IOException.class);
        }

        @Test
        void rejectsCertificatePemAsPrivateKey() {
            assertThatThrownBy(() -> PemParser.parsePrivateKey(certPem))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("Unexpected PEM object type");
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
                            .encodeToString(rsaKeyPair.getPrivate().getEncoded())
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
        void preservesChainOrder() throws Exception {
            KeyPair pair1 = CertificateGenerator.generateRsaKeyPair();
            KeyPair pair2 = CertificateGenerator.generateRsaKeyPair();
            X509Certificate cert1 = CertificateGenerator.generateSelfSignedX509Certificate(pair1);
            X509Certificate cert2 = CertificateGenerator.generateSelfSignedX509Certificate(pair2);
            byte[] pem1 = Files.readAllBytes(CertificateGenerator.generateCertPem(cert1));
            byte[] pem2 = Files.readAllBytes(CertificateGenerator.generateCertPem(cert2));

            byte[] multiPem = new byte[pem1.length + pem2.length];
            System.arraycopy(pem1, 0, multiPem, 0, pem1.length);
            System.arraycopy(pem2, 0, multiPem, pem1.length, pem2.length);

            X509Certificate[] chain = PemParser.parseCertificateChain(multiPem);
            assertThat(chain[0]).isEqualTo(cert1);
            assertThat(chain[1]).isEqualTo(cert2);
        }

        @Test
        void ignoresNonCertificateObjects() throws Exception {
            byte[] multiPem = new byte[pkcs8KeyPem.length + certPem.length];
            System.arraycopy(pkcs8KeyPem, 0, multiPem, 0, pkcs8KeyPem.length);
            System.arraycopy(certPem, 0, multiPem, pkcs8KeyPem.length, certPem.length);

            X509Certificate[] chain = PemParser.parseCertificateChain(multiPem);
            assertThat(chain).hasSize(1);
            assertThat(chain[0].getSubjectX500Principal()).isEqualTo(certificate.getSubjectX500Principal());
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

        @Test
        void rejectsPemContainingOnlyPrivateKey() {
            assertThatThrownBy(() -> PemParser.parseCertificateChain(pkcs8KeyPem))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("No certificates found");
        }
    }
}
