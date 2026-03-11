/*
 * Copyright Kroxylicious Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.kroxylicious.proxy.internal.tls;

import java.io.File;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Base64;

/**
 * Utility for generating test certificates using JDK's keytool subprocess.
 */
public final class TestCertificateUtil {

    private static final String ALIAS = "test";
    private static final String PASSWORD = "changeit";

    private TestCertificateUtil() {
    }

    /**
     * Generates a self-signed X509 certificate and key pair using keytool.
     */
    static X509Certificate generateSelfSignedCert(KeyPair ignored) throws Exception {
        return generateSelfSignedCert(ignored, "CN=localhost");
    }

    /**
     * Generates a self-signed X509 certificate via keytool and returns it.
     * The KeyPair parameter is used to generate a cert matching that key.
     * NOTE: This creates a NEW key pair internally (keytool generates its own).
     * For tests that need key/cert match, use {@link #generateKeyStoreAndCert()}.
     */
    static X509Certificate generateSelfSignedCert(KeyPair ignored, String dn) throws Exception {
        return generateKeyStoreAndCert(dn).cert;
    }

    public record KeyAndCert(PrivateKey privateKey, X509Certificate cert) {}

    /**
     * Generates a self-signed certificate and matching private key via keytool.
     */
    public static KeyAndCert generateKeyStoreAndCert() throws Exception {
        return generateKeyStoreAndCert("CN=localhost");
    }

    public static KeyAndCert generateKeyStoreAndCert(String dn) throws Exception {
        File ksFile = File.createTempFile("test-keystore", ".jks");
        ksFile.delete(); // keytool requires non-existent file
        ksFile.deleteOnExit();

        // Generate keystore with self-signed cert using keytool
        ProcessBuilder pb = new ProcessBuilder(
                "keytool", "-genkeypair",
                "-alias", ALIAS,
                "-keyalg", "RSA",
                "-keysize", "2048",
                "-validity", "365",
                "-dname", dn,
                "-keystore", ksFile.getAbsolutePath(),
                "-storepass", PASSWORD,
                "-keypass", PASSWORD,
                "-storetype", "JKS");
        pb.inheritIO();
        Process p = pb.start();
        if (p.waitFor() != 0) {
            throw new RuntimeException("keytool failed");
        }

        // Load from keystore
        KeyStore ks = KeyStore.getInstance("JKS");
        try (var is = new java.io.FileInputStream(ksFile)) {
            ks.load(is, PASSWORD.toCharArray());
        }

        PrivateKey key = (PrivateKey) ks.getKey(ALIAS, PASSWORD.toCharArray());
        X509Certificate cert = (X509Certificate) ks.getCertificate(ALIAS);
        return new KeyAndCert(key, cert);
    }

    /**
     * Generates a PEM-encoded certificate string.
     */
    static String toPem(X509Certificate cert) throws Exception {
        var sb = new StringBuilder();
        sb.append("-----BEGIN CERTIFICATE-----\n");
        sb.append(Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(cert.getEncoded()));
        sb.append("\n-----END CERTIFICATE-----\n");
        return sb.toString();
    }

    /**
     * Generates a PEM-encoded PKCS#8 private key string.
     */
    static String toPkcs8Pem(PrivateKey key) {
        var sb = new StringBuilder();
        sb.append("-----BEGIN PRIVATE KEY-----\n");
        sb.append(Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(key.getEncoded()));
        sb.append("\n-----END PRIVATE KEY-----\n");
        return sb.toString();
    }

    /**
     * Generates a PEM-encoded PKCS#1 (RSA PRIVATE KEY) string by running openssl conversion.
     * Falls back to extracting the PKCS#1 portion from PKCS#8 DER manually.
     */
    static String toPkcs1Pem(PrivateKey key) throws Exception {
        // PKCS#8 DER structure: SEQUENCE { version, AlgorithmIdentifier, OCTET STRING { PKCS#1 } }
        // We need to extract the OCTET STRING content which is the PKCS#1 key
        byte[] pkcs8 = key.getEncoded();

        // Parse ASN.1: skip SEQUENCE tag+length, skip version (3 bytes), skip AlgorithmIdentifier
        int pos = 0;
        // Skip outer SEQUENCE tag
        pos++; // tag 0x30
        pos += derLengthSize(pkcs8, pos);
        // Skip version INTEGER (0x02 0x01 0x00)
        pos += 3;
        // Skip AlgorithmIdentifier SEQUENCE
        pos++; // tag 0x30
        int algIdLen = derReadLength(pkcs8, pos);
        pos += derLengthSize(pkcs8, pos) + algIdLen;
        // Now we're at the OCTET STRING containing PKCS#1
        if (pkcs8[pos] != 0x04) {
            throw new IllegalStateException("Expected OCTET STRING tag (0x04) at position " + pos + ", got " + pkcs8[pos]);
        }
        pos++; // skip OCTET STRING tag
        int pkcs1Len = derReadLength(pkcs8, pos);
        pos += derLengthSize(pkcs8, pos);
        byte[] pkcs1 = new byte[pkcs1Len];
        System.arraycopy(pkcs8, pos, pkcs1, 0, pkcs1Len);

        var sb = new StringBuilder();
        sb.append("-----BEGIN RSA PRIVATE KEY-----\n");
        sb.append(Base64.getMimeEncoder(64, "\n".getBytes()).encodeToString(pkcs1));
        sb.append("\n-----END RSA PRIVATE KEY-----\n");
        return sb.toString();
    }

    private static int derReadLength(byte[] data, int pos) {
        if ((data[pos] & 0x80) == 0) {
            return data[pos] & 0x7F;
        }
        int numBytes = data[pos] & 0x7F;
        int length = 0;
        for (int i = 0; i < numBytes; i++) {
            length = (length << 8) | (data[pos + 1 + i] & 0xFF);
        }
        return length;
    }

    private static int derLengthSize(byte[] data, int pos) {
        if ((data[pos] & 0x80) == 0) {
            return 1;
        }
        return 1 + (data[pos] & 0x7F);
    }
}
