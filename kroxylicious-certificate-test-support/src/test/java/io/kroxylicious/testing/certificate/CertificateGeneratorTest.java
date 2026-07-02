/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.certificate;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.openssl.PEMException;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class CertificateGeneratorTest {

    @Test
    void rsaKeyPair() {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        assertRsaKeyPairGenerated(keyPair);
    }

    @Test
    void writeRsaPrivateKeyPem() throws IOException {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        Path path = CertificateGenerator.writeRsaPrivateKeyPem(keyPair);
        assertPemAtPathContainsPrivateKey(path, keyPair);
    }

    @Test
    void writeEncryptedRsaPrivateKeyPem() throws IOException {
        // Given: a key pair and password
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        String password = "testpassword";

        // When: writing encrypted PEM
        Path path = CertificateGenerator.writeEncryptedRsaPrivateKeyPem(keyPair, password);

        // Then: the private key can be decrypted with the password
        byte[] pemBytes = Files.readAllBytes(path);
        PrivateKey decrypted = PemParser.parsePrivateKey(pemBytes, password.toCharArray());
        assertThat(decrypted).isEqualTo(keyPair.getPrivate());
    }

    @Test
    void generateSelfSignedX509Certificate() {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate x509Certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        assertSelfSignedCertGeneratedForKeyPair(x509Certificate, keyPair);
    }

    @Test
    void generateCertPem() throws IOException {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate x509Certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        Path path = CertificateGenerator.generateCertPem(x509Certificate);
        Object pair = new PEMParser(new FileReader(path.toFile())).readObject();
        assertThat(pair).isInstanceOfSatisfying(X509CertificateHolder.class, x509CertificateHolder -> {
            X509Certificate certificate = convertToJcaX509Cert(x509CertificateHolder);
            assertThat(certificate).isEqualTo(x509Certificate);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = { "JKS", "PKCS12" })
    void createKeystoreCreatesCorrectType(String type) throws Exception {
        // Given: a key pair, certificate, and passwords
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        String storePassword = "storePazz";
        String keyPassword = "keyPazz";

        // When: creating keystore with specified type
        CertificateGenerator.KeyStore keyStore = CertificateGenerator.createKeystore(keyPair, certificate, storePassword, keyPassword, type);

        // Then: keystore contains the key and certificate with correct type and passwords
        assertKeyStoreContains(keyStore, keyPair, certificate, keyPassword, storePassword, type);
    }

    @Test
    void generateCreatesRsaKeyPair() {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create a valid RSA key pair
        assertRsaKeyPairGenerated(keys.serverKey());
    }

    @Test
    void generateCreatesUnencryptedPrivateKeyPem() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create unencrypted private key PEM matching the key pair
        assertPemAtPathContainsPrivateKey(keys.privateKeyPem(), keys.serverKey());
    }

    @Test
    void generateCreatesEncryptedPrivateKeyPem() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create encrypted private key PEM with password
        assertThat(keys.encryptedPrivateKeyPem()).isNotNull();
        assertThat(keys.encryptedPrivateKeyPassword()).isEqualTo(CertificateGenerator.ENCRYPTED_KEY_PASSWORD);
        byte[] encryptedPemBytes = Files.readAllBytes(keys.encryptedPrivateKeyPem());
        PrivateKey decryptedKey = PemParser.parsePrivateKey(encryptedPemBytes, keys.encryptedPrivateKeyPassword().toCharArray());
        assertThat(decryptedKey).isEqualTo(keys.serverKey().getPrivate());
    }

    @Test
    void generateCreatesSelfSignedCertificatePem() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create self-signed certificate PEM for the key pair
        assertThat(keys.selfSignedCertificatePem()).isNotNull();
        Object cert = new PEMParser(new FileReader(keys.selfSignedCertificatePem().toFile())).readObject();
        assertThat(cert).isInstanceOf(X509CertificateHolder.class);
        X509Certificate certificate = convertToJcaX509Cert((X509CertificateHolder) cert);
        assertSelfSignedCertGeneratedForKeyPair(certificate, keys.serverKey());
    }

    @Test
    void generateCreatesJksServerKeystore() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create JKS keystore containing the private key and certificate
        X509Certificate certificate = loadCertificateFromPem(keys.selfSignedCertificatePem());
        assertKeyStoreContains(keys.jksServerKeystore(), keys.serverKey(), certificate, "keypass", "changeit", CertificateGenerator.JKS);
    }

    @Test
    void generateCreatesJksClientTruststore() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create JKS truststore with password
        X509Certificate certificate = loadCertificateFromPem(keys.selfSignedCertificatePem());
        assertTrustStore(keys.jksClientTruststore(), certificate, "changeit", "JKS");
    }

    @Test
    void generateCreatesPkcs12ClientTruststore() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create PKCS12 truststore with password
        X509Certificate certificate = loadCertificateFromPem(keys.selfSignedCertificatePem());
        assertTrustStore(keys.pkcs12ClientTruststore(), certificate, "changeit", "PKCS12");
    }

    @Test
    void generateCreatesPkcs12NoPasswordClientTruststore() throws Exception {
        // When: calling generate
        CertificateGenerator.Keys keys = CertificateGenerator.generate();

        // Then: should create PKCS12 truststore without password
        X509Certificate certificate = loadCertificateFromPem(keys.selfSignedCertificatePem());
        assertTrustStore(keys.pkcs12NoPasswordClientTruststore(), certificate, null, "PKCS12");
    }

    private X509Certificate loadCertificateFromPem(Path pemPath) throws Exception {
        Object cert = new PEMParser(new FileReader(pemPath.toFile())).readObject();
        assertThat(cert).isInstanceOf(X509CertificateHolder.class);
        return convertToJcaX509Cert((X509CertificateHolder) cert);
    }

    private void assertTrustStore(CertificateGenerator.TrustStore trustStore, X509Certificate certificate, @Nullable String password, String type) throws Exception {
        assertThat(trustStore).isNotNull();
        assertThat(trustStore.password()).isEqualTo(password);
        if (password != null) {
            assertThat(trustStore.passwordFile()).isNotNull();
            assertThat(Files.readString(trustStore.passwordFile())).isEqualTo(password);
        }
        else {
            assertThat(trustStore.passwordFile()).isNull();
        }
        KeyStore keyStore = loadKeyStore(trustStore.path(), password, type);
        Certificate trustedCert = keyStore.getCertificate(CertificateGenerator.ALIAS);
        assertThat(trustedCert).isNotNull().isEqualTo(certificate);
    }

    private static void assertRsaKeyPairGenerated(KeyPair keyPair) {
        assertThat(keyPair).isNotNull();
        PublicKey publicKey = keyPair.getPublic();
        assertThat(publicKey.getAlgorithm()).isEqualTo("RSA");
        assertThat(publicKey.getFormat()).isEqualTo("X.509");
        PrivateKey privateKey = keyPair.getPrivate();
        assertThat(privateKey.getAlgorithm()).isEqualTo("RSA");
        assertThat(privateKey.getFormat()).isEqualTo("PKCS#8");
        assertThat(privateKey).isInstanceOfSatisfying(RSAPrivateKey.class, rsaPrivateKey -> assertThat(rsaPrivateKey.getModulus().bitLength()).isEqualTo(2048));
    }

    private static void assertPemAtPathContainsPrivateKey(Path path, KeyPair keyPair) throws IOException {
        assertThat(path).isNotNull();
        Object pair = new PEMParser(new FileReader(path.toFile())).readObject();
        assertThat(pair).isInstanceOfSatisfying(PEMKeyPair.class, pemKeyPair -> {
            KeyPair jcaKeyPair = convertToJcaKeyPair(pemKeyPair);
            assertThat(jcaKeyPair.getPublic()).isEqualTo(keyPair.getPublic());
            assertThat(jcaKeyPair.getPrivate()).isEqualTo(keyPair.getPrivate());
        });
    }

    private static KeyPair convertToJcaKeyPair(PEMKeyPair pemKeyPair) {
        try {
            return new JcaPEMKeyConverter().getKeyPair(pemKeyPair);
        }
        catch (PEMException e) {
            throw new RuntimeException(e);
        }
    }

    private static X509Certificate convertToJcaX509Cert(X509CertificateHolder x509CertificateHolder) {
        try {
            return new JcaX509CertificateConverter().getCertificate(x509CertificateHolder);
        }
        catch (CertificateException e) {
            throw new RuntimeException(e);
        }
    }

    private static void assertSelfSignedCertGeneratedForKeyPair(X509Certificate x509Certificate, KeyPair keyPair) {
        assertThat(x509Certificate).isNotNull();
        assertThat(x509Certificate.getSigAlgName()).isEqualTo("SHA256withRSA");
        assertThat(x509Certificate.getSubjectX500Principal().getName()).isEqualTo("CN=localhost");
        assertThat(x509Certificate.getIssuerX500Principal().getName()).isEqualTo("CN=localhost");
        assertThat(x509Certificate.getSerialNumber()).isEqualTo(BigInteger.valueOf(1));
        assertThat(x509Certificate.getNotBefore().toInstant()).isBefore(Instant.now());
        Instant approxAfter = Instant.now().plus(9999, ChronoUnit.DAYS).minus(5, ChronoUnit.SECONDS);
        assertThat(x509Certificate.getNotAfter().toInstant()).isAfter(approxAfter);
        assertThat(x509Certificate.getPublicKey().getEncoded()).isEqualTo(keyPair.getPublic().getEncoded());

        // Verifies that this certificate was signed using the private key that corresponds to the specified public key.
        assertThatCode(() -> x509Certificate.verify(keyPair.getPublic())).doesNotThrowAnyException();
    }

    private static void assertKeyStoreContains(CertificateGenerator.KeyStore jksKeystore, KeyPair keyPair, X509Certificate x509Certificate, String keyPassword,
                                               String storePassword, String type)
            throws Exception {
        assertThat(jksKeystore).isNotNull();
        assertThat(jksKeystore.keyPassword()).isNotNull().isEqualTo(keyPassword);
        assertThat(jksKeystore.keyPasswordFile()).isNotNull();
        assertThat(Files.readString(jksKeystore.keyPasswordFile())).isEqualTo(keyPassword);
        assertThat(jksKeystore.storePassword()).isNotNull().isEqualTo(storePassword);
        assertThat(jksKeystore.storePasswordFile()).isNotNull();
        assertThat(Files.readString(jksKeystore.storePasswordFile())).isEqualTo(storePassword);
        assertThat(jksKeystore.path()).isNotNull();
        assertThat(jksKeystore.type()).isEqualTo(type);
        KeyStore store = loadKeyStore(jksKeystore.path(), jksKeystore.storePassword(), type);
        Key key = store.getKey(CertificateGenerator.ALIAS, jksKeystore.keyPassword().toCharArray());
        assertThat(key).isEqualTo(keyPair.getPrivate());
        Certificate certificate = store.getCertificate(CertificateGenerator.ALIAS);
        assertThat(certificate).isEqualTo(x509Certificate);
    }

    private static @NonNull KeyStore loadKeyStore(Path path, @Nullable String password, String type) {
        try {
            KeyStore store = KeyStore.getInstance(type);
            store.load(Files.newInputStream(path), password == null ? null : password.toCharArray());
            return store;
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "JKS", "PKCS12" })
    void createTrustStoreCreatesCorrectType(String type) throws Exception {
        // Given: a certificate and password
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        String password = "trustpass";

        // When: building trust store with specified type
        CertificateGenerator.TrustStore trustStore = CertificateGenerator.createTrustStore(certificate, password, type);

        // Then: trust store contains the certificate with correct type and password
        assertTrustStore(trustStore, certificate, password, type);
    }

    @Test
    void createTrustStoreHandlesNullPasswordForPkcs12() throws Exception {
        // Given: a certificate with no password
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);

        // When: building PKCS12 trust store without password
        CertificateGenerator.TrustStore trustStore = CertificateGenerator.createTrustStore(certificate, null, "PKCS12");

        // Then: trust store is created without password
        assertThat(trustStore).isNotNull();
        assertThat(trustStore.password()).isNull();
        assertThat(trustStore.passwordFile()).isNull();
        KeyStore keyStore = loadKeyStore(trustStore.path(), null, "PKCS12");
        Certificate trustedCert = keyStore.getCertificate(CertificateGenerator.ALIAS);
        assertThat(trustedCert).isNotNull().isEqualTo(certificate);
    }

    @Test
    void createTrustStoreFailsWithInvalidType() {
        // Given: a certificate
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);

        // When/Then: building trust store with invalid type throws RuntimeException wrapping KeyStoreException
        assertThatCode(() -> CertificateGenerator.createTrustStore(certificate, "pass", "INVALID_TYPE"))
                .isInstanceOf(RuntimeException.class)
                .hasCauseInstanceOf(java.security.KeyStoreException.class)
                .hasMessageContaining("INVALID_TYPE");
    }

}
