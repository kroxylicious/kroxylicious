/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.tls;

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

    @Test
    void createJksKeystore() throws Exception {
        KeyPair keyPair = CertificateGenerator.generateRsaKeyPair();
        X509Certificate x509Certificate = CertificateGenerator.generateSelfSignedX509Certificate(keyPair);
        CertificateGenerator.KeyStore jksKeystore = CertificateGenerator.createJksKeystore(keyPair, x509Certificate, "storePazz", "keyPazz");
        assertKeyStoreContains(jksKeystore, keyPair, x509Certificate, "keyPazz", "storePazz");
    }

    @Test
    void generate() throws Exception {
        CertificateGenerator.Keys keys = CertificateGenerator.generate();
        assertRsaKeyPairGenerated(keys.serverKey());
        assertPemAtPathContainsPrivateKey(keys.privateKeyPem(), keys.serverKey());

        assertThat(keys.selfSignedCertificatePem()).isNotNull();
        Object cert = new PEMParser(new FileReader(keys.selfSignedCertificatePem().toFile())).readObject();
        assertThat(cert).isInstanceOf(X509CertificateHolder.class);
        X509Certificate certificate = convertToJcaX509Cert((X509CertificateHolder) cert);
        assertSelfSignedCertGeneratedForKeyPair(certificate, keys.serverKey());

        assertKeyStoreContains(keys.jksServerKeystore(), keys.serverKey(), certificate, "keypass", "changeit");

        assertTrustStore(keys.jksClientTruststore(), certificate, "changeit", "JKS");
        assertTrustStore(keys.pkcs12ClientTruststore(), certificate, "changeit", "PKCS12");
        assertTrustStore(keys.pkcs12NoPasswordClientTruststore(), certificate, null, "PKCS12");
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
        assertThat(privateKey).isInstanceOfSatisfying(RSAPrivateKey.class, rsaPrivateKey -> assertThat(rsaPrivateKey.getModulus().bitLength()).isEqualTo(1024));
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
                                               String storePassword)
            throws Exception {
        assertThat(jksKeystore).isNotNull();
        assertThat(jksKeystore.keyPassword()).isNotNull().isEqualTo(keyPassword);
        assertThat(jksKeystore.keyPasswordFile()).isNotNull();
        assertThat(Files.readString(jksKeystore.keyPasswordFile())).isEqualTo(keyPassword);
        assertThat(jksKeystore.storePassword()).isNotNull().isEqualTo(storePassword);
        assertThat(jksKeystore.storePasswordFile()).isNotNull();
        assertThat(Files.readString(jksKeystore.storePasswordFile())).isEqualTo(storePassword);
        assertThat(jksKeystore.path()).isNotNull();
        assertThat(jksKeystore.type()).isEqualTo("JKS");
        KeyStore store = loadKeyStore(jksKeystore.path(), jksKeystore.storePassword(), "JKS");
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

}
