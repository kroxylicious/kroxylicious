/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.testing.certificate;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.asn1.x509.SubjectPublicKeyInfo;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.X509v3CertificateBuilder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JcePEMEncryptorBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;

/**
 * Test utility to create key material in a variety of forms.
 */
public class CertificateGenerator {

    public static final String PKCS_12 = "PKCS12";
    public static final String JKS = "JKS";
    public static final String ALIAS = "alias";
    @SuppressWarnings("java:S2068") // This is a test password. A constant is not inappropriate.
    public static final String ENCRYPTED_KEY_PASSWORD = "keypass";

    private CertificateGenerator() {
        // utility class - do not construct
    }

    public static KeyPair generateRsaKeyPair() {
        try {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048, new SecureRandom());
            return generator.generateKeyPair();
        }
        catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path writeRsaPrivateKeyPem(KeyPair pair) {
        try {
            File rsakey = createTempFile("rsakey", ".pem");
            return writeToPem(pair.getPrivate(), rsakey);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static Path writeEncryptedRsaPrivateKeyPem(KeyPair pair, String password) {
        try {
            var rsakey = createTempFile("encrypted-rsakey", ".pem");
            try (var pemWriter = new JcaPEMWriter(Files.newBufferedWriter(rsakey.toPath(), StandardCharsets.UTF_8))) {
                var encryptorBuilder = new JcePEMEncryptorBuilder("AES-256-CBC");
                encryptorBuilder.setProvider(new BouncyCastleProvider());
                pemWriter.writeObject(pair.getPrivate(), encryptorBuilder.build(password.toCharArray()));
            }
            return rsakey.toPath();
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @NonNull
    private static Path writeToPem(Object obj, File file) throws IOException {
        try (JcaPEMWriter pemWriter = new JcaPEMWriter(new FileWriter(file, StandardCharsets.UTF_8))) {
            pemWriter.writeObject(obj);
        }
        return file.toPath();
    }

    @NonNull
    @SuppressWarnings("java:S5443") // These files are used exclusively on test path, file permissions aren't a concern
    private static File createTempFile(String prefix, String suffix) {
        try {
            File file = File.createTempFile(prefix, suffix);
            file.deleteOnExit();
            return file;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static Path generateCertPem(X509Certificate certificate) {
        try {
            File certFile = createTempFile("cert", ".pem");
            return writeToPem(certificate, certFile);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static X509Certificate generateSelfSignedX509Certificate(KeyPair pair) {
        try {
            var subPubKeyInfo = SubjectPublicKeyInfo.getInstance(pair.getPublic().getEncoded());
            var now = Instant.now();
            var validFrom = Date.from(now);
            var validTo = Date.from(now.plus(Duration.ofDays(9999)));
            var certBuilder = new X509v3CertificateBuilder(
                    // Currently it is important that the issuer name equals the subject name. We use these certs on vault to authenticate
                    // the client. We tell vault that the client certificate is a trusted CA. With client authentication enabled,
                    // vault sends a list of trusted issuers to the client as part of the handshake (the subject name from the cert).
                    // When the client is told about the trusted issuers it tries to locate a certificate with that issuer to present
                    // to the server. It's at that point that we need the issuer name to match what vault sent.
                    new X500Name("CN=localhost"),
                    BigInteger.ONE,
                    validFrom,
                    validTo,
                    new X500Name("CN=localhost"),
                    subPubKeyInfo);
            var signer = new JcaContentSignerBuilder("SHA256WithRSA")
                    .setProvider(new BouncyCastleProvider())
                    .build(pair.getPrivate());
            X509CertificateHolder holder = certBuilder.build(signer);
            JcaX509CertificateConverter converter = new JcaX509CertificateConverter();
            return converter.getCertificate(holder);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Create a trust store containing the given certificate.
     *
     * @param cert the certificate to include in the trust store
     * @param password optional password for the trust store
     * @param type the trust store type (e.g., "JKS", "PKCS12")
     * @return TrustStore record containing path, type, password, and password file
     */
    @NonNull
    public static TrustStore createTrustStore(X509Certificate cert, @Nullable String password, String type) {
        String suffix = PKCS_12.equals(type) ? ".p12" : ".jks";
        try {
            File certFile = createTempFile("trust", suffix);
            java.security.KeyStore store = java.security.KeyStore.getInstance(type);
            store.load(null, null);
            store.setCertificateEntry(ALIAS, cert);
            char[] pass = password != null ? password.toCharArray() : null;
            writeKeyStore(certFile, store, pass);
            Path passwordFile = password != null ? writeToTempFile(password) : null;
            return new TrustStore(certFile.toPath(), type, password, passwordFile);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public record TrustStore(@NonNull Path path, @NonNull String type, @Nullable String password, @Nullable Path passwordFile) {

    }

    public record KeyStore(@NonNull Path path, @NonNull String type, @Nullable String storePassword, @Nullable Path storePasswordFile, @Nullable String keyPassword,
                           @Nullable Path keyPasswordFile) {

    }

    public record Keys(KeyPair serverKey,
                       Path privateKeyPem,
                       Path encryptedPrivateKeyPem,
                       String encryptedPrivateKeyPassword,
                       Path selfSignedCertificatePem,
                       TrustStore pkcs12ClientTruststore,
                       TrustStore jksClientTruststore,
                       TrustStore pkcs12NoPasswordClientTruststore,
                       KeyStore jksServerKeystore) {}

    @SuppressWarnings("java:S2068") // java:S2068 concerns hardcoded passwords. This code is used exclusively in tests so it is considered acceptable.
    public static Keys generate() {
        String password = "changeit";
        KeyPair pair = generateRsaKeyPair();
        Path privateKeyPem = writeRsaPrivateKeyPem(pair);
        Path encryptedPrivateKeyPem = writeEncryptedRsaPrivateKeyPem(pair, ENCRYPTED_KEY_PASSWORD);
        X509Certificate x509Certificate = generateSelfSignedX509Certificate(pair);
        KeyStore keyStore = createJksKeystore(pair, x509Certificate, password, ENCRYPTED_KEY_PASSWORD);
        Path serverCert = generateCertPem(x509Certificate);
        TrustStore pkcs12ClientTruststore = createTrustStore(x509Certificate, password, PKCS_12);
        TrustStore pkcs12NoPasswordTruststore = createTrustStore(x509Certificate, null, PKCS_12);
        TrustStore jksClientTruststore = createTrustStore(x509Certificate, password, JKS);
        return new Keys(pair, privateKeyPem, encryptedPrivateKeyPem, ENCRYPTED_KEY_PASSWORD, serverCert, pkcs12ClientTruststore, jksClientTruststore,
                pkcs12NoPasswordTruststore, keyStore);
    }

    public static KeyStore createJksKeystore(KeyPair privateKeyPem, X509Certificate x509Certificate, String storePassword, String keyPassword) {
        try {
            File tempFile = createTempFile("keystore", "jks");
            java.security.KeyStore store = java.security.KeyStore.getInstance(JKS);
            store.load(null);
            store.setKeyEntry(ALIAS, privateKeyPem.getPrivate(), keyPassword.toCharArray(), new Certificate[]{ x509Certificate });
            writeKeyStore(tempFile, store, storePassword.toCharArray());
            Path path = tempFile.toPath();
            Path storePasswordFile = writeToTempFile(storePassword);
            Path keyPasswordFile = writeToTempFile(keyPassword);
            return new KeyStore(path, JKS, storePassword, storePasswordFile, keyPassword, keyPasswordFile);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void writeKeyStore(File tempFile, java.security.KeyStore store, char[] storePassword)
            throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException {
        try (var stream = Files.newOutputStream(tempFile.toPath())) {
            store.store(stream, storePassword);
        }
    }

    private static Path writeToTempFile(String password) {
        try {
            File tempFile = createTempFile("pass", "raw");
            Path path = tempFile.toPath();
            Files.writeString(path, password, StandardCharsets.UTF_8);
            return path;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
